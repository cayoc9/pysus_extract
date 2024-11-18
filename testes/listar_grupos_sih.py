# listar_grupos_sih.py

from pysus.online_data.SIH import SIH

def listar_grupos_sih():
    sih = SIH().load()
    grupos = sih.groups
    print("Grupos disponíveis para SIH:")
    for codigo, descricao in grupos.items():
        print(f"{codigo}: {descricao}")

if __name__ == "__main__":
    listar_grupos_sih()
