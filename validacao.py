import re
import sqlite3
import pandas as pd
import asyncio
import aiohttp
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QFileDialog,
    QTableWidget, QTableWidgetItem, QLabel, QHeaderView, QProgressBar, QApplication
)
from PyQt6.QtGui import QFont
from PyQt6.QtCore import Qt, QThread, pyqtSignal

# ---------------- Funções de validação ----------------
def validar_cpf(cpf: str) -> bool:
    cpf = re.sub(r'\D', '', str(cpf))
    if len(cpf) != 11 or cpf == cpf[0]*11:
        return False
    soma = sum(int(cpf[i])*(10-i) for i in range(9))
    digito1 = (soma*10 %11) %10
    soma = sum(int(cpf[i])*(11-i) for i in range(10))
    digito2 = (soma*10 %11) %10
    return digito1 == int(cpf[9]) and digito2 == int(cpf[10])

def validar_cnpj(cnpj: str) -> bool:
    cnpj = re.sub(r'\D', '', str(cnpj))
    if len(cnpj)!=14 or cnpj==cnpj[0]*14:
        return False
    pesos1=[5,4,3,2,9,8,7,6,5,4,3,2]
    soma1=sum(int(cnpj[i])*pesos1[i] for i in range(12))
    digito1=0 if soma1%11<2 else 11-(soma1%11)
    pesos2=[6]+pesos1
    soma2=sum(int(cnpj[i])*pesos2[i] for i in range(13))
    digito2=0 if soma2%11<2 else 11-(soma2%11)
    return digito1==int(cnpj[12]) and digito2==int(cnpj[13])

# ----------------- Async CEP Worker com Batches -----------------
CACHE_DB = "cache_ceps.db"
MAX_CONCURRENT = 50
MAX_RETRIES = 3
BATCH_SIZE = 5000  # Processa 5k CEPs por vez

async def fetch_cep(session, cep, cache_conn):
    cur = cache_conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS cache (cep TEXT PRIMARY KEY, valido INTEGER)")
    cur.execute("SELECT valido FROM cache WHERE cep=?", (cep,))
    row = cur.fetchone()
    if row is not None:
        return bool(row[0])

    valido=False
    for _ in range(MAX_RETRIES):
        try:
            async with session.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=10) as r:
                data = await r.json()
                if "erro" not in data:
                    valido=True
                    break
        except:
            pass
        if not valido:
            try:
                async with session.get(f"https://brasilapi.com.br/api/cep/v1/{cep}", timeout=10) as r2:
                    data2 = await r2.json()
                    if "cep" in data2:
                        valido=True
                        break
            except:
                pass
    cur.execute("INSERT OR REPLACE INTO cache (cep,valido) VALUES (?,?)",(cep,int(valido)))
    cache_conn.commit()
    return valido

async def process_batch(batch, sem, session, cache_conn, progress_cb, resultados):
    tasks=[]
    for idx,row in batch.iterrows():
        cep = re.sub(r'\D','',str(row.get("CEP","")))
        doc = row.get("CNPJ/CPF","")
        valido_doc = row.get("Valido CPF/CNPJ", False)
        async def worker(cep=cep, doc=doc, valido_doc=valido_doc):
            async with sem:
                valido_cep = await fetch_cep(session, cep, cache_conn)
                resultados.append([doc, cep, valido_doc, valido_cep])
                progress_cb()
        tasks.append(worker())
    await asyncio.gather(*tasks)

async def validar_ceps_async(df, progress_callback):
    resultados=[]
    sem=asyncio.Semaphore(MAX_CONCURRENT)
    conn = sqlite3.connect(CACHE_DB)
    async with aiohttp.ClientSession() as session:
        total=len(df)
        for start in range(0,total,BATCH_SIZE):
            batch = df.iloc[start:start+BATCH_SIZE]
            await process_batch(batch, sem, session, conn, progress_callback, resultados)
    conn.close()
    return pd.DataFrame(resultados, columns=["CNPJ/CPF","CEP","Valido CPF/CNPJ","Valido CEP"])

# ----------------- QThread para rodar async sem travar GUI -----------------
class CEPWorker(QThread):
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(pd.DataFrame)

    def __init__(self, df):
        super().__init__()
        self.df = df
        self.done=0

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        def progress_cb():
            self.done+=1
            self.progress_signal.emit(self.done)
        df_final = loop.run_until_complete(validar_ceps_async(self.df, progress_cb))
        self.finished_signal.emit(df_final)
        loop.close()

# ---------------- Classe principal ----------------
class ValidacaoApp(QWidget):
    def __init__(self):
        super().__init__()
        self.df=None
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()

        # Título
        titulo = QLabel("🧾 Validação de CPF/CNPJ e CEPs")
        titulo.setFont(QFont("Segoe UI",20,QFont.Weight.Bold))
        titulo.setStyleSheet("color:#f0f0f0;")
        layout.addWidget(titulo)

        # Botões principais
        top_buttons = QHBoxLayout()
        self.btn_db = QPushButton("🗄️ Importar SQLite")
        self.btn_db.setStyleSheet("background-color:#4e8cff;color:white;border-radius:6px;padding:6px 14px;font-weight:600;")
        self.btn_db.clicked.connect(self.importar_sqlite)
        top_buttons.addWidget(self.btn_db)

        self.btn_validar_docs = QPushButton("✅ Validar CPF/CNPJ")
        self.btn_validar_docs.setStyleSheet("background-color:#2ecc71;color:white;border-radius:6px;padding:6px 14px;font-weight:600;")
        self.btn_validar_docs.clicked.connect(self.validar_documentos)
        top_buttons.addWidget(self.btn_validar_docs)

        self.btn_validar_ceps = QPushButton("🌐 Validar CEPs")
        self.btn_validar_ceps.setStyleSheet("background-color:#3498db;color:white;border-radius:6px;padding:6px 14px;font-weight:600;")
        self.btn_validar_ceps.clicked.connect(self.validar_ceps)
        top_buttons.addWidget(self.btn_validar_ceps)

        self.btn_export_validos = QPushButton("💾 Exportar Válidos")
        self.btn_export_validos.setStyleSheet("background-color:#27ae60;color:white;border-radius:6px;padding:6px 14px;font-weight:600;")
        self.btn_export_validos.clicked.connect(lambda: self.exportar(True))
        top_buttons.addWidget(self.btn_export_validos)

        self.btn_export_invalidos = QPushButton("💾 Exportar Inválidos")
        self.btn_export_invalidos.setStyleSheet("background-color:#e74c3c;color:white;border-radius:6px;padding:6px 14px;font-weight:600;")
        self.btn_export_invalidos.clicked.connect(lambda: self.exportar(False))
        top_buttons.addWidget(self.btn_export_invalidos)

        self.btn_limpar = QPushButton("🧹 Limpar Dados")
        self.btn_limpar.setStyleSheet("background-color:#7f8c8d;color:white;border-radius:6px;padding:6px 14px;font-weight:600;")
        self.btn_limpar.clicked.connect(self.limpar_dados)
        top_buttons.addWidget(self.btn_limpar)

        layout.addLayout(top_buttons)

        # Barra de progresso
        self.progress = QProgressBar()
        self.progress.setVisible(False)
        layout.addWidget(self.progress)

        # Contador
        self.lbl_contador = QLabel("Registros válidos: 0 | inválidos: 0")
        self.lbl_contador.setFont(QFont("Segoe UI",12))
        self.lbl_contador.setStyleSheet("color:#f0f0f0;")
        layout.addWidget(self.lbl_contador)

        # Tabela
        self.table = QTableWidget()
        self.table.setStyleSheet("""
            QTableWidget {background-color:#2a2a3d;color:#f0f0f0;gridline-color:#444;font-size:14px;}
            QHeaderView::section {background-color:#252536;padding:5px;border:none;color:#f0f0f0;}
            QTableWidget::item:selected {background-color:#1abc9c;color:#fff;}
            QTableWidget::item:hover {background-color:#34495e;}
        """)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.table)

        self.setLayout(layout)
        self.setStyleSheet("background-color:#1e1e2f;color:#f0f0f0;font-family:'Segoe UI';")

    # ---------------- Funções ----------------
    def importar_sqlite(self):
        path,_ = QFileDialog.getOpenFileName(self,"Banco SQLite","","*.db")
        if not path: return
        conn = sqlite3.connect(path)
        query = "SELECT id, mod_data, codigo_cliente, cep_original, cep_tratado, cidade, rua, bairro, numero, estado, cnpj_cpf, nome_razao FROM ceps"
        self.df = pd.read_sql_query(query,conn)
        conn.close()
        self.df.rename(columns={"cnpj_cpf":"CNPJ/CPF","cep_tratado":"CEP"}, inplace=True)
        self.preencher_tabela()

    def validar_documentos(self):
        if self.df is None: return
        resultados=[]
        for _, row in self.df.iterrows():
            doc=row.get("CNPJ/CPF","")
            num=re.sub(r'\D','',str(doc))
            valido_doc=validar_cpf(num) if len(num)<=11 else validar_cnpj(num)
            cep=row.get("CEP","")
            valido_cep=len(re.sub(r'\D','',str(cep)))==8
            resultados.append([doc, cep, valido_doc, valido_cep])
        self.df=pd.DataFrame(resultados, columns=["CNPJ/CPF","CEP","Valido CPF/CNPJ","Valido CEP"])
        self.preencher_tabela()

    def validar_ceps(self):
        if self.df is None: return
        self.progress.setVisible(True)
        self.progress.setMaximum(len(self.df))
        self.progress.setValue(0)
        self.worker = CEPWorker(self.df)
        self.worker.progress_signal.connect(self.progress.setValue)
        self.worker.finished_signal.connect(self.ceps_finalizados)
        self.worker.start()

    def ceps_finalizados(self, df_final):
        self.df = df_final
        validos = self.df["Valido CEP"].sum()
        invalidos = len(self.df)-validos
        self.lbl_contador.setText(f"Registros válidos: {validos} | inválidos: {invalidos}")
        self.preencher_tabela()
        self.progress.setVisible(False)

    def preencher_tabela(self):
        if self.df is None: return
        headers=list(self.df.columns)
        self.table.setColumnCount(len(headers))
        self.table.setHorizontalHeaderLabels(headers)
        self.table.setRowCount(len(self.df))
        for i,row in enumerate(self.df.values):
            for j,val in enumerate(row):
                item=QTableWidgetItem(str(val))
                if headers[j] in ["Valido CPF/CNPJ","Valido CEP"] and str(val)=="False":
                    item.setBackground(Qt.GlobalColor.red)
                self.table.setItem(i,j,item)

    def exportar(self, validos=True):
        if self.df is None: return
        path,_=QFileDialog.getSaveFileName(self,"Salvar","","*.csv")
        if not path: return
        if validos:
            df_export = self.df[(self.df["Valido CPF/CNPJ"]==True) & (self.df["Valido CEP"]==True)]
        else:
            df_export = self.df[(self.df["Valido CPF/CNPJ"]==False) | (self.df["Valido CEP"]==False)]
        df_export.to_csv(path,sep=";",index=False)

    def limpar_dados(self):
        self.table.clearContents()
        self.table.setRowCount(0)
        self.table.setColumnCount(0)
        self.lbl_contador.setText("Registros válidos: 0 | inválidos: 0")
        self.progress.setVisible(False)
        self.df = None

# ---------------- Execução ----------------
if __name__=="__main__":
    import sys
    app = QApplication(sys.argv)
    w = ValidacaoApp()
    w.setWindowTitle("Validação de CPF/CNPJ e CEPs")
    w.setGeometry(100,100,1300,700)
    w.show()
    sys.exit(app.exec())