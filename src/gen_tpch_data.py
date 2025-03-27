import duckdb
import os
import shutil
import argparse
from datetime import datetime


def duckdb_type_to_hive_type(duckdb_type):
    """将DuckDB数据类型转换为Hive兼容的数据类型"""
    duckdb_type = duckdb_type.upper()

    type_mapping = {
        'INTEGER': 'INT',
        'INT': 'INT',
        'BIGINT': 'BIGINT',
        'SMALLINT': 'SMALLINT',
        'TINYINT': 'TINYINT',
        'DECIMAL': 'DECIMAL',
        'NUMERIC': 'DECIMAL',
        'REAL': 'FLOAT',
        'FLOAT': 'FLOAT',
        'DOUBLE': 'DOUBLE',
        'VARCHAR': 'STRING',
        'CHAR': 'STRING',
        'TEXT': 'STRING',
        'STRING': 'STRING',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'DATE',
        'TIMESTAMP': 'TIMESTAMP',
        'TIME': 'STRING',
        'BLOB': 'BINARY',
        'BINARY': 'BINARY',
    }

    if 'DECIMAL' in duckdb_type and '(' in duckdb_type:
        return duckdb_type.replace('DECIMAL', 'DECIMAL')

    if 'VARCHAR' in duckdb_type and '(' in duckdb_type:
        return 'STRING'
    if 'CHAR' in duckdb_type and '(' in duckdb_type:
        return 'STRING'

    return type_mapping.get(duckdb_type, 'STRING')


def get_table_schema(conn, table_name):
    """获取表的结构信息"""
    result = conn.execute(f"DESCRIBE {table_name}").fetchall()

    columns = []
    for row in result:
        column_name = row[0]
        column_type = row[1]
        hive_type = duckdb_type_to_hive_type(column_type)
        columns.append((column_name, hive_type))

    return columns


def generate_hive_ddl(table_name, columns, is_external=True, location=None):
    """生成Hive DDL语句，支持内部表和外部表"""
    table_type = "EXTERNAL" if is_external else ""
    ddl = f"CREATE {table_type} TABLE IF NOT EXISTS {table_name} (\n"

    # 添加列定义
    column_defs = []
    for col_name, col_type in columns:
        column_defs.append(f"  {col_name} {col_type}")

    ddl += ",\n".join(column_defs)

    # 设置为Parquet格式，并指定位置
    ddl += "\n)\nSTORED AS PARQUET"

    # 如果提供了位置并且是外部表，则添加LOCATION子句
    if location and is_external:
        ddl += f"\nLOCATION '{location}/{table_name}'"

    ddl += ";"

    return ddl


def generate_tpch_dataset(output_dir="tpch", scale_factor=1):
    """
    生成TPC-H数据集，导出为Parquet，生成Hive DDL，并整理文件

    参数:
    output_dir: 输出目录
    scale_factor: TPC-H比例因子（默认为1，即SF1）
    """
    start_time = datetime.now()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始生成TPC-H SF{scale_factor}数据集...")

    # 创建临时和最终输出目录
    temp_dir = "temp_tpch"
    parquet_dir = os.path.join(output_dir, "parquet")
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(parquet_dir, exist_ok=True)

    # 创建临时数据库
    temp_db = os.path.join(temp_dir, "tpch_sf1.db")

    # 连接到DuckDB并生成数据
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 连接DuckDB并生成TPC-H数据...")
    con = duckdb.connect(temp_db)
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    con.execute(f"CALL dbgen(sf={scale_factor})")

    # 获取所有表名
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [table[0] for table in tables]

    # 创建Hive DDL文件
    ddl_file = os.path.join(output_dir, "tpch_sf1_hive.hql")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 生成Hive DDL文件: {ddl_file}")

    with open(ddl_file, "w") as f:
        f.write(f"-- Hive DDL for TPC-H SF{scale_factor}\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # 创建数据库语句
        f.write("-- 创建数据库\n")
        f.write("CREATE DATABASE IF NOT EXISTS tpch_sf1;\n")
        f.write("USE tpch_sf1;\n\n")

        # 为每个表生成DDL
        for table_name in table_names:
            # 获取表的行数
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 处理表: {table_name} ({row_count} 行)")

            # 创建表的Parquet目录
            table_dir = os.path.join(parquet_dir, table_name)
            os.makedirs(table_dir, exist_ok=True)

            # 导出表为Parquet
            parquet_file = os.path.join(table_dir, f"{table_name}.parquet")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 导出 {table_name} 到 {parquet_file}")
            con.execute(f"COPY {table_name} TO '{parquet_file}' (FORMAT PARQUET)")

            # 获取表结构
            columns = get_table_schema(con, table_name)

            # 生成内部表DDL
            ddl = generate_hive_ddl(table_name, columns, is_external=False)

            # 写入DDL到文件
            f.write(f"-- Table: {table_name} ({row_count} rows)\n")
            f.write(ddl)
            f.write("\n\n")

    # 关闭连接
    con.close()

    # 创建加载数据的脚本
    load_script = os.path.join(output_dir, "load_tpch_data.sh")
    with open(load_script, "w") as f:
        f.write("#!/bin/bash\n\n")
        f.write("# 脚本用于加载TPC-H SF1数据到Hive内部表\n\n")

        f.write("# 设置数据目录\n")
        f.write('TPCH_DATA_DIR="$1"\n\n')

        f.write('if [ -z "$TPCH_DATA_DIR" ]; then\n')
        f.write('    echo "请提供数据目录路径作为参数"\n')
        f.write('    echo "用法: ./load_tpch_data.sh /path/to/tpch/data"\n')
        f.write('    exit 1\n')
        f.write('fi\n\n')

        f.write("# 首先创建表结构\n")
        f.write('echo "创建Hive表结构..."\n')
        f.write('hive -f "$TPCH_DATA_DIR/tpch_sf1_hive.hql"\n\n')

        f.write("# 然后加载数据到表中\n")
        f.write('echo "加载数据到Hive表..."\n')
        f.write('for table_dir in "$TPCH_DATA_DIR/parquet"/*; do\n')
        f.write('    table_name=$(basename "$table_dir")\n')
        f.write('    parquet_file="$table_dir/$table_name.parquet"\n')
        f.write('    echo "正在加载 $table_name 数据..."\n')
        f.write('    # 将parquet文件移动到HDFS临时位置\n')
        f.write('    hdfs dfs -mkdir -p /tmp/tpch_load\n')
        f.write('    hdfs dfs -put -f "$parquet_file" /tmp/tpch_load/\n')
        f.write('    # 使用LOAD DATA命令加载数据到表中\n')
        f.write(
            '    hive -e "LOAD DATA INPATH \'/tmp/tpch_load/$table_name.parquet\' OVERWRITE INTO TABLE tpch_sf1.$table_name;"\n')
        f.write('done\n\n')

        f.write('# 清理HDFS临时目录\n')
        f.write('echo "清理临时文件..."\n')
        f.write('hdfs dfs -rm -r -skipTrash /tmp/tpch_load\n\n')

        f.write('echo "数据加载完成!"\n')

    # 添加执行权限
    os.chmod(load_script, 0o755)

    # 删除临时目录和文件
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 清理临时文件...")
    shutil.rmtree(temp_dir, ignore_errors=True)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 处理完成！")
    print(f"总耗时: {duration:.2f} 秒")
    print(f"TPC-H SF{scale_factor}数据已保存到: {os.path.abspath(output_dir)}")
    print(f"Parquet文件位置: {os.path.abspath(parquet_dir)}")
    print(f"Hive DDL文件: {os.path.abspath(ddl_file)}")
    print(f"加载脚本: {os.path.abspath(load_script)}")
    print("\n使用方法:")
    print(f"  ./load_tpch_data.sh /path/to/tpch/data")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='生成TPC-H数据集并导出为Parquet和Hive DDL')
    parser.add_argument('-o', '--output-dir', default='tpch', help='输出目录 (默认: tpch)')
    parser.add_argument('-s', '--scale-factor', type=float, default=1, help='TPC-H比例因子 (默认: 1)')

    args = parser.parse_args()
    generate_tpch_dataset(args.output_dir, args.scale_factor)