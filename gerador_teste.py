import pandas as pd
import random
from datetime import datetime, timedelta

# ---------------- Config ----------------
NUM_REGISTROS = 1000000

cidades = ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba", "Salvador"]
estados = ["SP", "RJ", "MG", "PR", "BA"]
ruas = ["Rua A", "Rua B", "Av. Central", "Rua das Flores", "Av. Brasil"]
bairros = ["Centro", "Jardim", "Vila Nova", "Industrial", "Comercial"]
nomes = ["Empresa Alpha", "Empresa Beta", "João Silva", "Maria Souza", "Tech Solutions"]

# ---------------- Funções auxiliares ----------------
def gerar_cep():
    return str(random.randint(10000000, 99999999))

def gerar_cpf_cnpj():
    return str(random.randint(10000000000, 99999999999))  # fake

def gerar_codigo():
    return f"C{random.randint(1000,9999)}"

def gerar_data():
    base = datetime.now()
    return (base - timedelta(days=random.randint(0, 200))).strftime("%d/%m/%Y")

# ---------------- Gerar dados ----------------
dados = []

for _ in range(NUM_REGISTROS):
    idx = random.randint(0, len(cidades)-1)

    dados.append({
        "modificação: data": gerar_data(),
        "Código cliente C005": gerar_codigo(),
        "CEP": gerar_cep(),
        "Cidade": cidades[idx],
        "Rua": random.choice(ruas),
        "Bairro": random.choice(bairros),
        "Número": random.randint(1, 999),
        "Estado": estados[idx],
        "CNPJ/CPF": gerar_cpf_cnpj(),
        "Nome / Razão Social": random.choice(nomes)
    })

# ---------------- Criar DataFrame ----------------
df = pd.DataFrame(dados)

# ---------------- Salvar CSV ----------------
df.to_csv("dados_teste.csv", sep=";", index=False, encoding="utf-8-sig")

print("CSV gerado com sucesso: dados_teste.csv")