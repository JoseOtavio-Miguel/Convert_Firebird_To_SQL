import subprocess
import os
import fdb
import os

os.environ['FIREBIRD'] = r"C:\Firebird_2_5"
os.environ['PATH'] += r";C:\Firebird_2_5\bin"


# ==============================
# CONFIGURAÇÕES
# ==============================

FBK_PATH = r"C:\sql_normalize\fbk\backup.fbk"
FDB_PATH = r"C:\sql_normalize\fdb_convertido\database.fdb"


FIREBIRD_BIN = r"C:\Firebird_2_5\bin"

GBAK = os.path.join(FIREBIRD_BIN, "gbak.exe")
ISQL = os.path.join(FIREBIRD_BIN, "isql.exe")

USER = "SYSDBA"
PASSWORD = "masterkey"

# ==============================
# FUNÇÃO PARA EXECUTAR COMANDOS
# ==============================
import subprocess

def run_command(command):
    print("\n==============================")
    print("COMANDO:")
    print(command)
    print("==============================\n")

    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

<<<<<<< HEAD
    # Remove o .fdb se já existir 
    if Path(FDB_PATH).exists():
        print(" Arquivo FDB já existe, removendo...")
        os.remove(FDB_PATH)
=======
    stdout, stderr = process.communicate()
>>>>>>> parent of 4b8dbda (Unificação da conversão em FDB e correção da SQL)

    print("STDOUT:\n", stdout)
    print("STDERR:\n", stderr)
    print("RETURN CODE:", process.returncode)

    if process.returncode != 0:
        raise Exception("Erro real acima no STDERR!")

# ==============================
# 1. RESTORE FBK → FDB
# ==============================

<<<<<<< HEAD
        print("\n Saída:")
        print(processo.stdout)
=======
def restore_backup():
    command = (
        f'"{GBAK}" -rep -v '
        f'-user {USER} -password {PASSWORD} '
        f'-role NONE '
        f'-fix_fss_metadata ISO8859_1 '
        f'"{FBK_PATH}" "C:\\sql_normalize\\fdb_convertido\\database.fdb"'
    )
>>>>>>> parent of 4b8dbda (Unificação da conversão em FDB e correção da SQL)

    run_command(command)

def dump_metadata():
    output_file = r"C:\sql_normalize\metadata.sql"

    command = (
        f'"{ISQL}" -user {USER} -password {PASSWORD} '
        f'-role NONE "{FDB_PATH}" -a > "{output_file}"'
    )

    run_command(command)


def dump_data_python():

    con = fdb.connect(
        dsn=FDB_PATH,
        user=USER,
        password=PASSWORD,
        fb_library_name=r"C:\Program Files\Firebird_2_5\bin\fbclient.dll"
    )

    cur = con.cursor()

    # pegar tabelas reais
    cur.execute("""
        SELECT RDB$RELATION_NAME 
        FROM RDB$RELATIONS 
        WHERE RDB$SYSTEM_FLAG = 0
        AND RDB$VIEW_BLR IS NULL
    """)

    tables = [row[0].strip() for row in cur.fetchall()]

    output = r"C:/sql_normalize/dados.sql"

    with open(output, "w", encoding="utf-8") as f:
        for table in tables:
            print(f"Dumpando tabela: {table}")

            cur.execute(f"SELECT * FROM {table}")
            cols = [desc[0].strip() for desc in cur.description]

            for row in cur.fetchall():
                values = []
                for val in row:
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, str):
                        val = val.replace("'", "''")
                        values.append(f"'{val}'")
                    else:
                        values.append(str(val))

                insert = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(values)});\n"
                f.write(insert)

    con.close()

def limpar_banco_com_gbak():
    clean_fbk = r"C:\sql_normalize\clean.fbk"
    clean_fdb = r"C:\sql_normalize\database_clean.fdb"

    # backup ignorando problemas
    cmd1 = (
        f'"{GBAK}" -b -v -ignore '
        f'-user {USER} -password {PASSWORD} '
        f'"{FDB_PATH}" "{clean_fbk}"'
    )
    run_command(cmd1)

    # restore limpo
    cmd2 = (
        f'"{GBAK}" -c -v '
        f'-user {USER} -password {PASSWORD} '
        f'"{clean_fbk}" "{clean_fdb}"'
    )
    run_command(cmd2)

    return clean_fdb

def limpar_udf():
    sql = """
    DROP EXTERNAL FUNCTION SU$APPENDBLOBTOFILE;
    DROP EXTERNAL FUNCTION SU$FILEEXISTS;
    DROP EXTERNAL FUNCTION SU$DELETEFILE;
    COMMIT;
    """

    temp_sql = r"C:\sql_normalize\clean_udf.sql"

    with open(temp_sql, "w") as f:
        f.write(sql)

    command = (
        f'"{ISQL}" -user {USER} -password {PASSWORD} '
        f'"{FDB_PATH}" -i "{temp_sql}"'
    )

    run_command(command)

# ==============================
# MAIN
# ==============================


if __name__ == "__main__":
    restore_backup()
    FDB_PATH = limpar_banco_com_gbak()
    dump_metadata()
    dump_data_python()