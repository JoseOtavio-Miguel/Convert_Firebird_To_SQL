import subprocess

USER = 'DAIRYLAND'
PASSWORD = 'masterkey'
BANCO = r"C:/sql_convert/backup/restaurado_2.fdb"

def remover_trigger_onconnect():
    print("[STEP] Removendo trigger TR_ONCONNECT")

    isql_path = r"C:\Program Files (x86)\Firebird\Firebird_2_5\bin\isql.exe"

    script = f"""
    CONNECT '{BANCO}' USER '{USER}' PASSWORD '{PASSWORD}';
    SET TERM ^ ;
    DROP TRIGGER TR_ONCONNECT ^
    SET TERM ; ^
    COMMIT;
    QUIT;
    """

    try:
        processo = subprocess.Popen(
            [isql_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        out, err = processo.communicate(script)

        print(out)

        if err:
            print("[WARN]", err)

        print("[OK] Trigger removido (ou não existia)")

    except Exception as e:
        print("[ERRO] Falha ao remover trigger:", e)