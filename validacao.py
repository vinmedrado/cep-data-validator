import re
import sqlite3
from datetime import datetime

from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QFileDialog,
    QTableView, QLabel, QProgressBar, QApplication, QMessageBox
)
from PyQt6.QtGui import QFont, QColor
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QAbstractTableModel, QModelIndex


# ---------------- Validação de documentos ----------------
def validar_cpf(cpf: str) -> bool:
    cpf = re.sub(r'\D', '', str(cpf))
    if len(cpf) != 11 or cpf == cpf[0]*11:
        return False
    soma = sum(int(cpf[i])*(10-i) for i in range(9))
    digito1 = (soma*10 % 11) % 10
    soma = sum(int(cpf[i])*(11-i) for i in range(10))
    digito2 = (soma*10 % 11) % 10
    return digito1 == int(cpf[9]) and digito2 == int(cpf[10])

def validar_cnpj(cnpj: str) -> bool:
    cnpj = re.sub(r'\D', '', str(cnpj))
    if len(cnpj) != 14 or cnpj == cnpj[0]*14:
        return False
    pesos1 = [5,4,3,2,9,8,7,6,5,4,3,2]
    soma1 = sum(int(cnpj[i])*pesos1[i] for i in range(12))
    digito1 = 0 if soma1%11 < 2 else 11-(soma1%11)
    pesos2 = [6]+pesos1
    soma2 = sum(int(cnpj[i])*pesos2[i] for i in range(13))
    digito2 = 0 if soma2%11 < 2 else 11-(soma2%11)
    return digito1 == int(cnpj[12]) and digito2 == int(cnpj[13])


# ---------------- Async CEP (importado só quando usado) ----------------
CACHE_DB = "cache_ceps.db"
MAX_CONCURRENT = 50
MAX_RETRIES = 3
BATCH_SIZE = 5000

def inicializar_cache(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS cache (cep TEXT PRIMARY KEY, valido INTEGER)")
    conn.commit()

async def fetch_cep(session, cep, cache_conn):
    import aiohttp
    cur = cache_conn.cursor()
    cur.execute("SELECT valido FROM cache WHERE cep=?", (cep,))
    row = cur.fetchone()
    if row is not None:
        return bool(row[0])

    # FIX: ClientTimeout correto para aiohttp
    timeout = aiohttp.ClientTimeout(total=10)
    valido = False

    for _ in range(MAX_RETRIES):
        try:
            async with session.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=timeout) as r:
                data = await r.json()
                if "erro" not in data:
                    valido = True
                    break
        except Exception:
            pass
        if not valido:
            try:
                async with session.get(f"https://brasilapi.com.br/api/cep/v1/{cep}", timeout=timeout) as r2:
                    data2 = await r2.json()
                    if "cep" in data2:
                        valido = True
                        break
            except Exception:
                pass

    cur.execute("INSERT OR REPLACE INTO cache (cep, valido) VALUES (?,?)", (cep, int(valido)))
    cache_conn.commit()
    return valido

async def process_batch(batch, sem, session, cache_conn, progress_cb, resultados):
    tasks = []
    for idx, row in batch.iterrows():
        cep = re.sub(r'\D', '', str(row.get("CEP", "")))
        doc = row.get("CNPJ/CPF", "")
        # FIX: coluna pode não existir antes de validar documentos
        valido_doc = row.get("Valido CPF/CNPJ", None)

        async def worker(cep=cep, doc=doc, valido_doc=valido_doc):
            async with sem:
                valido_cep = await fetch_cep(session, cep, cache_conn)
                resultados.append([doc, cep, valido_doc, valido_cep])
                progress_cb()

        tasks.append(worker())
    await asyncio.gather(*tasks)

async def validar_ceps_async(df, progress_callback):
    import asyncio
    import aiohttp
    resultados = []
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    conn = sqlite3.connect(CACHE_DB)
    inicializar_cache(conn)  # FIX: cria tabela uma única vez

    async with aiohttp.ClientSession() as session:
        total = len(df)
        for start in range(0, total, BATCH_SIZE):
            batch = df.iloc[start:start+BATCH_SIZE]
            await process_batch(batch, sem, session, conn, progress_callback, resultados)

    conn.close()
    return df.__class__(resultados, columns=["CNPJ/CPF", "CEP", "Valido CPF/CNPJ", "Valido CEP"])


# ---------------- QThread ----------------
class CEPWorker(QThread):
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(object)  # object para evitar import pandas no topo

    def __init__(self, df):
        super().__init__()
        self.df = df
        self.done = 0

    def run(self):
        import asyncio
        import pandas as pd

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        def progress_cb():
            self.done += 1
            self.progress_signal.emit(self.done)

        df_final = loop.run_until_complete(validar_ceps_async(self.df, progress_cb))
        # garante que é DataFrame antes de emitir
        if not hasattr(df_final, 'columns'):
            df_final = pd.DataFrame(df_final, columns=["CNPJ/CPF", "CEP", "Valido CPF/CNPJ", "Valido CEP"])
        self.finished_signal.emit(df_final)
        loop.close()

class DocWorker(QThread):
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(object)

    def __init__(self, df):
        super().__init__()
        self.df = df
        self.done = 0

    def run(self):
        import pandas as pd

        resultados = []

        for _, row in self.df.iterrows():
            doc = row.get("CNPJ/CPF", "")
            num = re.sub(r'\D', '', str(doc))

            valido_doc = validar_cpf(num) if len(num) <= 11 else validar_cnpj(num)

            cep = row.get("CEP", "")
            valido_cep = len(re.sub(r'\D', '', str(cep))) == 8

            resultados.append([doc, cep, valido_doc, valido_cep])

            self.done += 1
            self.progress_signal.emit(self.done)

        df_final = pd.DataFrame(resultados, columns=[
            "CNPJ/CPF", "CEP", "Valido CPF/CNPJ", "Valido CEP"
        ])

        self.finished_signal.emit(df_final)


# ---------------- Modelo de Tabela ----------------
class PandasModel(QAbstractTableModel):
    def __init__(self, df=None):
        super().__init__()
        import pandas as pd
        self._df = df if df is not None else pd.DataFrame()

    def rowCount(self, parent=QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QModelIndex()):
        return len(self._df.columns)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        val = self._df.iloc[index.row(), index.column()]
        if role == Qt.ItemDataRole.DisplayRole:
            return str(val)
        if role == Qt.ItemDataRole.BackgroundRole:
            col = self._df.columns[index.column()]
            if col in ["Valido CPF/CNPJ", "Valido CEP"] and str(val) == "False":
                return QColor("#e74c3c")
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            return str(self._df.columns[section])
        return str(section+1)


# ---------------- App principal ----------------
class ValidacaoApp(QWidget):
    def __init__(self):
        super().__init__()
        self.df = None
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()

        titulo = QLabel("🧾 Validação de CPF/CNPJ e CEPs")
        titulo.setFont(QFont("Segoe UI", 20, QFont.Weight.Bold))
        titulo.setStyleSheet("color:#f0f0f0;")
        layout.addWidget(titulo)

        top_buttons = QHBoxLayout()

        btns = [
            ("btn_db",              "🗄️ Importar SQLite",  "#4e8cff", self.importar_sqlite),
            ("btn_validar_docs",    "✅ Validar CPF/CNPJ", "#2ecc71", self.validar_documentos),
            ("btn_validar_ceps",    "🌐 Validar CEPs",     "#3498db", self.validar_ceps),
            ("btn_export_validos",  "💾 Exportar Válidos", "#27ae60", lambda: self.exportar(True)),
            ("btn_export_invalidos","💾 Exportar Inválidos","#e74c3c", lambda: self.exportar(False)),
            ("btn_limpar",          "🧹 Limpar Dados",     "#7f8c8d", self.limpar_dados),
        ]
        for attr, label, color, slot in btns:
            btn = QPushButton(label)
            btn.setStyleSheet(
                f"background-color:{color};color:white;border-radius:6px;"
                "padding:6px 14px;font-weight:600;"
            )
            btn.clicked.connect(slot)
            setattr(self, attr, btn)
            top_buttons.addWidget(btn)

        layout.addLayout(top_buttons)

        self.progress = QProgressBar()
        self.progress.setVisible(False)
        layout.addWidget(self.progress)

        self.lbl_contador = QLabel("Registros válidos: 0 | inválidos: 0")
        self.lbl_contador.setFont(QFont("Segoe UI", 12))
        self.lbl_contador.setStyleSheet("color:#f0f0f0;")
        layout.addWidget(self.lbl_contador)

        self.table = QTableView()
        layout.addWidget(self.table)

        self.setLayout(layout)
        self.setStyleSheet("background-color:#1e1e2f;color:#f0f0f0;font-family:'Segoe UI';")

    # ---------------- Funções ----------------
    def importar_sqlite(self):
        # pandas importado só aqui
        import pandas as pd
        start = datetime.now()
        path, _ = QFileDialog.getOpenFileName(self, "Banco SQLite", "", "*.db")
        if not path:
            return
        conn = sqlite3.connect(path)
        self.df = pd.read_sql_query(
            "SELECT id, mod_data, codigo_cliente, cep_original, cep_tratado, "
            "cidade, rua, bairro, numero, estado, cnpj_cpf, nome_razao FROM ceps",
            conn
        )
        conn.close()
        self.df.rename(columns={
            "id": "ID",
            "mod_data": "Data Modificação",
            "codigo_cliente": "Código Cliente",
            "cep_original": "CEP Original",
            "cep_tratado": "CEP",
            "cidade": "Cidade",
            "rua": "Rua",
            "bairro": "Bairro",
            "numero": "Número",
            "estado": "Estado",
            "nome_razao": "Nome",
            "cnpj_cpf": "CNPJ/CPF"
        }, inplace=True)
        self.atualizar_tabela()
        print(f"[IMPORTAÇÃO] {(datetime.now()-start).total_seconds():.2f}s")


    def validar_documentos(self):
        if self.df is None:
            return

        start = datetime.now()

        # mostra barra
        self.progress.setVisible(True)
        self.progress.setMaximum(len(self.df))
        self.progress.setValue(0)
        self.progress.setFormat("Validando documentos... %p%")

        # cria worker
        self.worker_doc = DocWorker(self.df)
        self.worker_doc.progress_signal.connect(self.progress.setValue)

        def on_finished(df_final, start=start):
            self.df = df_final
            self.atualizar_tabela()
            self.progress.setVisible(False)

            print(f"[VALIDAÇÃO DOC] {(datetime.now()-start).total_seconds():.2f}s")

        self.worker_doc.finished_signal.connect(on_finished)
        self.worker_doc.start()

    def validar_ceps(self):
        if self.df is None:
            return
        start = datetime.now()
        self.progress.setVisible(True)
        self.progress.setMaximum(len(self.df))
        self.progress.setValue(0)

        self.worker = CEPWorker(self.df)
        self.worker.progress_signal.connect(self.progress.setValue)

        def on_finished(df_final, start=start):
            self.ceps_finalizados(df_final)
            print(f"[VALIDAÇÃO CEP] {(datetime.now()-start).total_seconds():.2f}s")

        self.worker.finished_signal.connect(on_finished)
        self.worker.start()

    def ceps_finalizados(self, df_final):
        self.df = df_final
        validos = int(self.df["Valido CEP"].sum())
        invalidos = len(self.df) - validos
        self.lbl_contador.setText(f"Registros válidos: {validos} | inválidos: {invalidos}")
        self.atualizar_tabela()
        self.progress.setVisible(False)

    def atualizar_tabela(self):
        if self.df is None:
            return

        df_view = self.df.copy()

        # esconder colunas na interface
        colunas_ocultar = ["CEP Original"]

        for col in colunas_ocultar:
            if col in df_view.columns:
                df_view.drop(columns=[col], inplace=True)

        self.table.setModel(PandasModel(df_view))

    def exportar(self, validos=True):
        if self.df is None:
            return

        # FIX: verifica colunas antes de filtrar para evitar KeyError
        colunas_necessarias = ["Valido CPF/CNPJ", "Valido CEP"]
        faltando = [c for c in colunas_necessarias if c not in self.df.columns]
        if faltando:
            QMessageBox.warning(
                self, "Atenção",
                f"Execute a validação antes de exportar.\nColunas ausentes: {', '.join(faltando)}"
            )
            return

        start = datetime.now()
        path, _ = QFileDialog.getSaveFileName(self, "Salvar", "", "*.csv")
        if not path:
            return

        if validos:
            df_export = self.df[(self.df["Valido CPF/CNPJ"] == True) & (self.df["Valido CEP"] == True)]
        else:
            df_export = self.df[(self.df["Valido CPF/CNPJ"] == False) | (self.df["Valido CEP"] == False)]

        df_export.to_csv(path, sep=";", index=False)
        print(f"[EXPORTAÇÃO {'VÁLIDOS' if validos else 'INVÁLIDOS'}] {(datetime.now()-start).total_seconds():.2f}s")

    def limpar_dados(self):
        import pandas as pd
        self.table.setModel(PandasModel(pd.DataFrame()))
        self.lbl_contador.setText("Registros válidos: 0 | inválidos: 0")
        self.progress.setVisible(False)
        self.df = None


# ---------------- Execução standalone ----------------
if __name__ == "__main__":
    import sys
    from PyQt6.QtWidgets import QApplication
    app = QApplication(sys.argv)
    w = ValidacaoApp()
    w.setWindowTitle("Validação de CPF/CNPJ e CEPs")
    w.setGeometry(100, 100, 1300, 700)
    w.show()
    sys.exit(app.exec())