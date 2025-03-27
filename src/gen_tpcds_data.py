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


def generate_hive_ddl(table_name, columns):
    """生成Hive内部表DDL语句"""
    ddl = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    column_defs = []
    for col_name, col_type in columns:
        column_defs.append(f"  {col_name} {col_type}")
    ddl += ",\n".join(column_defs)
    ddl += "\n)\nSTORED AS PARQUET;"
    return ddl


def generate_tpcds_dataset(output_dir="tpcds", scale_factor=1):
    """
    生成TPC-DS数据集，导出为Parquet，生成Hive DDL，并整理文件
    参数:
    output_dir: 输出目录
    scale_factor: TPC-DS比例因子（默认为1，即SF1）
    """
    start_time = datetime.now()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 开始生成TPC-DS SF{scale_factor}数据集...")
    
    temp_dir = "temp_tpcds"
    parquet_dir = os.path.join(output_dir, "parquet")
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(parquet_dir, exist_ok=True)
    
    temp_db = os.path.join(temp_dir, "tpcds_sf1.db")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 连接DuckDB并生成TPC-DS数据...")
    
    con = duckdb.connect(temp_db)
    con.execute("INSTALL tpcds")
    con.execute("LOAD tpcds")
    con.execute(f"CALL dsdgen(sf={scale_factor})")
    
    tables = con.execute("SHOW TABLES").fetchall()
    table_names = [table[0] for table in tables]
    
    ddl_file = os.path.join(output_dir, "tpcds_sf1_hive.hql")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 生成Hive DDL文件: {ddl_file}")
    with open(ddl_file, "w") as f:
        f.write(f"-- Hive DDL for TPC-DS SF{scale_factor}\n")
        f.write(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write("-- 创建数据库\n")
        f.write("CREATE DATABASE IF NOT EXISTS tpcds_sf1;\n")
        f.write("USE tpcds_sf1;\n\n")
        
        for table_name in table_names:
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 处理表: {table_name} ({row_count} 行)")
            
            table_dir = os.path.join(parquet_dir, table_name)
            os.makedirs(table_dir, exist_ok=True)
            parquet_file = os.path.join(table_dir, f"{table_name}.parquet")
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 导出 {table_name} 到 {parquet_file}")
            con.execute(f"COPY {table_name} TO '{parquet_file}' (FORMAT PARQUET)")
            
            columns = get_table_schema(con, table_name)
            
            # 生成内部表DDL
            ddl = generate_hive_ddl(table_name, columns)
            f.write(f"-- Table: {table_name} ({row_count} rows)\n")
            f.write(ddl)
            f.write("\n\n")
    
    con.close()
    
    # 生成加载脚本
    load_script = os.path.join(output_dir, "load_tpcds_data.sh")
    with open(load_script, "w") as f:
        f.write("#!/bin/bash\n\n")
        f.write("# 脚本用于加载TPC-DS SF1数据到Hive内部表\n\n")
        
        f.write("# 设置数据目录\n")
        f.write('TPCDS_DATA_DIR="$1"\n\n')
        
        f.write('if [ -z "$TPCDS_DATA_DIR" ]; then\n')
        f.write('    echo "请提供数据目录路径作为参数"\n')
        f.write('    echo "用法: ./load_tpcds_data.sh /path/to/tpcds/data"\n')
        f.write('    exit 1\n')
        f.write('fi\n\n')
        
        f.write("# 首先创建表结构\n")
        f.write('echo "创建Hive表结构..."\n')
        f.write('hive -f "$TPCDS_DATA_DIR/tpcds_sf1_hive.hql"\n\n')
        
        f.write("# 然后加载数据到表中\n")
        f.write('echo "加载数据到Hive表..."\n')
        f.write('for table_dir in "$TPCDS_DATA_DIR/parquet"/*; do\n')
        f.write('    table_name=$(basename "$table_dir")\n')
        f.write('    parquet_file="$table_dir/$table_name.parquet"\n')
        f.write('    echo "正在加载 $table_name 数据..."\n')
        f.write('    # 将parquet文件移动到HDFS临时位置\n')
        f.write('    hdfs dfs -mkdir -p /tmp/tpcds_load\n')
        f.write('    hdfs dfs -put -f "$parquet_file" /tmp/tpcds_load/\n')
        f.write('    # 使用LOAD DATA命令加载数据到表中\n')
        f.write('    hive -e "LOAD DATA INPATH \'/tmp/tpcds_load/$table_name.parquet\' OVERWRITE INTO TABLE tpcds_sf1.$table_name;"\n')
        f.write('done\n\n')
        
        f.write('# 清理HDFS临时目录\n')
        f.write('echo "清理临时文件..."\n')
        f.write('hdfs dfs -rm -r -skipTrash /tmp/tpcds_load\n\n')
        
        f.write('echo "数据加载完成!"\n')
    
    os.chmod(load_script, 0o755)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 清理临时文件...")
    shutil.rmtree(temp_dir, ignore_errors=True)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 处理完成！")
    print(f"总耗时: {duration:.2f} 秒")
    print(f"TPC-DS SF{scale_factor}数据已保存到: {os.path.abspath(output_dir)}")
    print(f"Parquet文件位置: {os.path.abspath(parquet_dir)}")
    print(f"Hive DDL文件: {os.path.abspath(ddl_file)}")
    print(f"加载脚本: {os.path.abspath(load_script)}")
    print("\n使用方法:")
    print(f"  ./load_tpcds_data.sh /path/to/tpcds/data")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='生成TPC-DS数据集并导出为Parquet和Hive DDL')
    parser.add_argument('-o', '--output-dir', default='tpcds', help='输出目录 (默认: tpcds)')
    parser.add_argument('-s', '--scale-factor', type=float, default=1, help='TPC-DS比例因子 (默认: 1)')
    args = parser.parse_args()
    
    generate_tpcds_dataset(args.output_dir, args.scale_factor)