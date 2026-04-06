import sys
import re
import os
import sqlite3
from datetime import datetime

from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLineEdit,
    QFileDialog, QTableView, QMessageBox, QLabel, QSlider
)
from PyQt6.QtGui import QKeySequence, QFont
from PyQt6.QtCore import Qt, QAbstractTableModel, QModelIndex, QVariant
from PyQt6.QtGui import QShortcut


# ---------------- Utils ----------------
def get_conn(db_path):
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


# ---------------- Banco ----------------
class BancoCeps:
    def __init__(self, db_path):
        self.db_path = db_path

    def criar_tabelas(self):
        with get_conn(self.db_path) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS ceps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mod_data TEXT,
                codigo_cliente TEXT,
                cep_original TEXT,
                cep_tratado TEXT,
                cidade TEXT,
                rua TEXT,
                bairro TEXT,
                numero TEXT,
                estado TEXT,
                cnpj_cpf TEXT,
                nome_razao TEXT,
                UNIQUE(codigo_cliente, cep_tratado)
            )
            """)
            conn.execute("""
            CREATE TABLE IF NOT EXISTS historico_buscas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cep TEXT,
                origem TEXT,
                data_hora TEXT
            )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cep ON ceps(cep_tratado)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_codigo ON ceps(codigo_cliente)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_data ON ceps(mod_data)")

    def upsert(self, df):
        cols = ["mod_data", "codigo_cliente", "cep_original", "cep_tratado",
                "cidade", "rua", "bairro", "numero", "estado", "cnpj_cpf", "nome_razao"]
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        dados = [tuple(row) for row in df[cols].to_numpy()]
        sql = """
        INSERT INTO ceps (mod_data,codigo_cliente,cep_original,cep_tratado,
            cidade,rua,bairro,numero,estado,cnpj_cpf,nome_razao)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(codigo_cliente, cep_tratado) DO UPDATE SET
            mod_data=excluded.mod_data,
            cep_original=excluded.cep_original,
            cidade=excluded.cidade,
            rua=excluded.rua,
            bairro=excluded.bairro,
            numero=excluded.numero,
            estado=excluded.estado,
            cnpj_cpf=excluded.cnpj_cpf,
            nome_razao=excluded.nome_razao
        """
        with get_conn(self.db_path) as conn:
            conn.executemany(sql, dados)
            conn.commit()

    def buscar_ceps_bloco(self, limit=1000, offset=0):
        with get_conn(self.db_path) as conn:
            return conn.execute(f"""
                SELECT mod_data,codigo_cliente,cep_original,cidade,rua,
                       bairro,numero,estado,cnpj_cpf,nome_razao
                FROM ceps
                ORDER BY mod_data DESC
                LIMIT {limit} OFFSET {offset}
            """).fetchall()

    def contar_ceps(self):
        with get_conn(self.db_path) as conn:
            res = conn.execute("SELECT COUNT(*) FROM ceps").fetchone()
            return res[0] if res else 0

    def buscar_cep(self, cep):
        with get_conn(self.db_path) as conn:
            return conn.execute("""
                SELECT mod_data,codigo_cliente,cep_original,cidade,rua,
                       bairro,numero,estado,cnpj_cpf,nome_razao
                FROM ceps WHERE cep_tratado=?
                LIMIT 1000
            """, (cep,)).fetchall()

    def buscar_codigo(self, codigo):
        with get_conn(self.db_path) as conn:
            return conn.execute("""
                SELECT mod_data,codigo_cliente,cep_original,cidade,rua,
                       bairro,numero,estado,cnpj_cpf,nome_razao
                FROM ceps WHERE codigo_cliente=?
                LIMIT 1000
            """, (codigo,)).fetchall()

    def buscar_filtrado(self, cidade="", estado=""):
        query = """
            SELECT mod_data,codigo_cliente,cep_original,cidade,rua,
                   bairro,numero,estado,cnpj_cpf,nome_razao
            FROM ceps WHERE 1=1
        """
        params = []
        if cidade:
            query += " AND LOWER(cidade) LIKE ?"
            params.append(f"%{cidade}%")
        if estado:
            query += " AND LOWER(estado) LIKE ?"
            params.append(f"%{estado}%")
        query += " ORDER BY mod_data DESC LIMIT 1000"
        with get_conn(self.db_path) as conn:
            return conn.execute(query, params).fetchall()

    def buscar_historico(self):
        with get_conn(self.db_path) as conn:
            return conn.execute("""
                SELECT cep, origem, data_hora
                FROM historico_buscas ORDER BY id DESC LIMIT 200
            """).fetchall()

    def top_ceps(self):
        with get_conn(self.db_path) as conn:
            return conn.execute("""
                SELECT cep, COUNT(*) FROM historico_buscas
                GROUP BY cep ORDER BY COUNT(*) DESC LIMIT 5
            """).fetchall()

    def ultima_data(self):
        with get_conn(self.db_path) as conn:
            res = conn.execute("SELECT MAX(mod_data) FROM ceps").fetchone()
            return res[0] if res and res[0] else "Sem registros"

    def salvar_busca(self, cep, origem):
        with get_conn(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT cep FROM historico_buscas ORDER BY id DESC LIMIT 1")
            last = cur.fetchone()
            if last and last[0] == cep:
                return
            cur.execute("""
                INSERT INTO historico_buscas (cep, origem, data_hora)
                VALUES (?, ?, ?)
            """, (cep, origem, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


# ---------------- Modelo Lazy ----------------
class LazyTableModel(QAbstractTableModel):
    def __init__(self, db, limit=1000, parent=None):
        super().__init__(parent)
        self.db = db
        self.limit = limit
        self.offset = 0
        self.dados = []
        self.total = self.db.contar_ceps()
        self.headers = ["Data", "Código", "CEP", "Cidade", "Rua", "Bairro", "Número", "Estado", "CNPJ", "Nome"]
        self.carregar_bloco()

    def rowCount(self, parent=QModelIndex()):
        return self.total

    def columnCount(self, parent=QModelIndex()):
        return len(self.headers)

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return QVariant()
        row, col = index.row(), index.column()
        if role == Qt.ItemDataRole.DisplayRole:
            if row >= len(self.dados):
                self.carregar_bloco(offset=row)
            if row < len(self.dados):
                return str(self.dados[row][col])
        return QVariant()

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role == Qt.ItemDataRole.DisplayRole and orientation == Qt.Orientation.Horizontal:
            return self.headers[section]
        return super().headerData(section, orientation, role)

    def carregar_bloco(self, offset=None):
        if offset is not None:
            self.offset = offset
        blocos = self.db.buscar_ceps_bloco(self.limit, self.offset)
        for i, row in enumerate(blocos):
            if self.offset + i < len(self.dados):
                self.dados[self.offset + i] = row
            else:
                self.dados.append(row)

    def aplicar_filtro(self, cidade="", estado=""):
        self.beginResetModel()
        self.dados = self.db.buscar_filtrado(cidade.lower(), estado.lower())
        self.total = len(self.dados)
        self.endResetModel()


# ---------------- App ----------------
class App(QWidget):
    def __init__(self):
        super().__init__()

        self.db_path = os.path.join(os.getcwd(), "ceps.db")
        self.db = BancoCeps(self.db_path)
        self.db.criar_tabelas()

        self.initUI()

        self.modelo = LazyTableModel(self.db, limit=1000)
        self.table.setModel(self.modelo)

        self.lbl_contador.setText(f"Registros: {self.db.contar_ceps()}")
        self.lbl_ultima_data.setText(f"Última data: {self.db.ultima_data()}")

    def initUI(self):
        layout = QVBoxLayout()

        topo_label = QHBoxLayout()
        titulo = QLabel("📍 Sistema de Consultas ViaCEP / Correios")
        titulo.setFont(QFont("Segoe UI", 20, QFont.Weight.Bold))
        titulo.setStyleSheet("color:#f0f0f0;")
        topo_label.addWidget(titulo)

        self.lbl_contador = QLabel("Registros: 0")
        self.lbl_contador.setFont(QFont("Segoe UI", 12))
        self.lbl_contador.setStyleSheet("color:#f0f0f0; padding-left:20px;")
        topo_label.addWidget(self.lbl_contador)

        self.lbl_ultima_data = QLabel("Última data: -")
        self.lbl_ultima_data.setFont(QFont("Segoe UI", 12))
        self.lbl_ultima_data.setStyleSheet("color:#f0f0f0; padding-left:20px;")
        topo_label.addWidget(self.lbl_ultima_data)

        topo_label.addStretch()
        layout.addLayout(topo_label)

        top = QHBoxLayout()
        self.btn_csv = self.criar_botao("📂 Importar CSV", "#4e8cff", "Importe um arquivo CSV com CEPs")
        self.btn_csv.clicked.connect(self.importar_csv)
        self.btn_export = self.criar_botao("💾 Exportar", "#2ecc71", "Exporte a tabela atual para CSV")
        self.btn_export.clicked.connect(self.exportar)
        self.btn_hist = self.criar_botao("🕒 Histórico", "#9b59b6", "Veja o histórico de buscas")
        self.btn_hist.clicked.connect(self.ver_historico)
        self.btn_stats = self.criar_botao("📊 Estatísticas", "#e67e22", "Veja os CEPs mais buscados")
        self.btn_stats.clicked.connect(self.ver_stats)
        self.btn_abrir_pasta = self.criar_botao("📂 Abrir Pasta do Banco", "#3498db", "Abrir a pasta onde o banco está salvo")
        self.btn_abrir_pasta.clicked.connect(self.abrir_pasta_banco)
        for btn in [self.btn_csv, self.btn_export, self.btn_hist, self.btn_stats, self.btn_abrir_pasta]:
            top.addWidget(btn)
        layout.addLayout(top)

        search = QHBoxLayout()
        self.input_cep = QLineEdit()
        self.input_cep.setPlaceholderText("Digite CEP(s) ou Código(s) (use ';' para múltiplas buscas)")
        self.input_cep.setStyleSheet(self.input_style())
        self.btn_buscar = self.criar_botao("🔎 Buscar", "#1abc9c", "Buscar CEP ou Código")
        self.btn_buscar.clicked.connect(self.buscar)
        self.btn_viacep = self.criar_botao("🌐 ViaCEP", "#c0392b", "Buscar CEP diretamente no ViaCEP")
        self.btn_viacep.clicked.connect(self.buscar_viacep)
        search.addWidget(self.input_cep)
        search.addWidget(self.btn_buscar)
        search.addWidget(self.btn_viacep)
        layout.addLayout(search)

        filtro = QHBoxLayout()
        self.input_cidade = QLineEdit()
        self.input_cidade.setPlaceholderText("Cidade")
        self.input_cidade.setStyleSheet(self.input_style())
        self.input_estado = QLineEdit()
        self.input_estado.setPlaceholderText("Estado")
        self.input_estado.setStyleSheet(self.input_style())
        self.btn_filtrar = self.criar_botao("🔍 Filtrar", "#2980b9", "Filtrar CEPs por Cidade/Estado")
        self.btn_filtrar.clicked.connect(self.filtrar)
        self.btn_limpar = self.criar_botao("🧹 Limpar", "#7f8c8d", "Limpar filtros")
        self.btn_limpar.clicked.connect(self.limpar_filtro)
        filtro.addWidget(self.input_cidade)
        filtro.addWidget(self.input_estado)
        filtro.addWidget(self.btn_filtrar)
        filtro.addWidget(self.btn_limpar)
        layout.addLayout(filtro)

        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.setSortingEnabled(True)
        self.table.setSelectionBehavior(QTableView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableView.SelectionMode.ExtendedSelection)
        layout.addWidget(self.table)

        slider_layout = QHBoxLayout()
        slider_label = QLabel("Altura da Tabela:")
        slider_label.setStyleSheet("color:#f0f0f0;")
        self.slider = QSlider(Qt.Orientation.Horizontal)
        self.slider.setMinimum(200)
        self.slider.setMaximum(1000)
        self.slider.setValue(400)
        self.slider.valueChanged.connect(lambda val: self.table.setFixedHeight(val))
        slider_layout.addWidget(slider_label)
        slider_layout.addWidget(self.slider)
        layout.addLayout(slider_layout)
        self.table.setFixedHeight(self.slider.value())

        copy_shortcut = QShortcut(QKeySequence("Ctrl+C"), self.table)
        copy_shortcut.activated.connect(self.copiar_tabela)

        self.btn_correios = self.criar_botao("🏤 Abrir Correios", "#f1c40f", "Abrir site dos Correios")
        self.btn_correios.clicked.connect(self.correios)
        layout.addWidget(self.btn_correios)

        self.setLayout(layout)
        self.setStyleSheet("background-color:#1e1e2f;color:#f0f0f0;font-family:'Segoe UI';")

    def criar_botao(self, text, color, tooltip):
        btn = QPushButton(text)
        btn.setStyleSheet(f"""
            QPushButton {{
                background-color:{color};
                color:white;
                border-radius:6px;
                padding:6px 14px;
                font-weight:600;
            }}
            QPushButton:hover {{
                background-color:{self.hover_color(color)};
                color:black;
            }}
        """)
        btn.setToolTip(tooltip)
        btn.setCursor(Qt.CursorShape.PointingHandCursor)
        return btn

    def hover_color(self, color):
        import colorsys
        color = color.lstrip('#')
        r, g, b = tuple(int(color[i:i+2], 16)/255.0 for i in (0, 2, 4))
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        l = min(1.0, l+0.15)
        r, g, b = colorsys.hls_to_rgb(h, l, s)
        return f'#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}'

    def input_style(self):
        return """
        QLineEdit {
            background-color:#2a2a3d;
            color:#f0f0f0;
            border:1px solid #444;
            border-radius:6px;
            padding:6px;
        }"""

    def copiar_tabela(self):
        selecionados = self.table.selectionModel().selectedIndexes()
        if not selecionados:
            return
        selecionados = sorted(selecionados, key=lambda x: (x.row(), x.column()))
        texto = ""
        linha_atual = selecionados[0].row()
        for index in selecionados:
            if index.row() != linha_atual:
                texto += "\n"
                linha_atual = index.row()
            else:
                if texto and not texto.endswith("\n"):
                    texto += "\t"
            texto += str(index.data() or "")
        QApplication.clipboard().setText(texto)

    def importar_csv(self):
        # Pandas importado só quando necessário
        import pandas as pd
        import unicodedata

        path, _ = QFileDialog.getOpenFileName(self, "CSV", "", "*.csv")
        if not path:
            return
        df = pd.read_csv(path, sep=";", dtype=str, encoding="utf-8-sig")
        df.columns = [self.clean_column(c) for c in df.columns]
        df.rename(columns={
            "modificacao_data": "mod_data",
            "codigo_cliente_c005": "codigo_cliente",
            "cep": "cep_original",
            "cnpj_cpf": "cnpj_cpf",
            "nome_razao_social": "nome_razao"
        }, inplace=True)
        if "mod_data" in df.columns:
            df["mod_data"] = pd.to_datetime(df["mod_data"], errors="coerce", dayfirst=True)
            df["mod_data"] = df["mod_data"].fillna(pd.Timestamp.now())
            df["mod_data"] = df["mod_data"].dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            df["mod_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if "cep_original" not in df.columns:
            QMessageBox.critical(self, "Erro", "Coluna CEP não encontrada!")
            return
        df["cep_tratado"] = df["cep_original"].fillna("").apply(self.formatar_cep)
        self.db.upsert(df)
        self.modelo = LazyTableModel(self.db, limit=1000)
        self.table.setModel(self.modelo)
        self.lbl_contador.setText(f"Registros: {self.db.contar_ceps()}")
        self.lbl_ultima_data.setText(f"Última data: {self.db.ultima_data()}")

    def buscar(self):
        texto = self.input_cep.text()
        if not texto.strip():
            QMessageBox.warning(self, "Erro", "Informe pelo menos um CEP ou Código")
            return
        valores = re.split(r"[\n;,]", texto)
        resultados = []
        for v in valores:
            v = v.strip()
            if not v:
                continue
            # FIX: exige exatamente 8 dígitos para buscar como CEP
            if v.isdigit() and len(v) == 8:
                res = self.db.buscar_cep(v)
            else:
                res = self.db.buscar_codigo(v)
            resultados.extend(res)
            self.db.salvar_busca(v, "Manual")
        if resultados:
            self.modelo.beginResetModel()
            self.modelo.dados = resultados
            self.modelo.total = len(resultados)
            self.modelo.endResetModel()
        else:
            QMessageBox.information(self, "Nenhum Resultado", "Nenhum registro encontrado.")

    def filtrar(self):
        self.modelo.aplicar_filtro(self.input_cidade.text(), self.input_estado.text())

    def limpar_filtro(self):
        self.input_cidade.setText("")
        self.input_estado.setText("")
        self.modelo = LazyTableModel(self.db, limit=1000)
        self.table.setModel(self.modelo)

    def ver_historico(self):
        historico = self.db.buscar_historico()
        msg = "\n".join([f"{h[0]} - {h[1]} - {h[2]}" for h in historico])
        QMessageBox.information(self, "Histórico", msg if msg else "Sem histórico.")

    def ver_stats(self):
        top = self.db.top_ceps()
        msg = "\n".join([f"{t[0]} - {t[1]} vezes" for t in top])
        QMessageBox.information(self, "CEPs Mais Buscados", msg if msg else "Sem registros.")

    def abrir_pasta_banco(self):
        os.startfile(os.path.dirname(self.db_path))

    def correios(self):
        import webbrowser
        webbrowser.open("https://buscacepinter.correios.com.br/app/endereco/index.php?t")

    def buscar_viacep(self):
        # requests importado só quando necessário
        import requests
        cep = self.input_cep.text().strip().replace("-", "")
        if not cep:
            QMessageBox.warning(self, "Erro", "Informe um CEP para buscar no ViaCEP")
            return
        try:
            res = requests.get(f"https://viacep.com.br/ws/{cep}/json/", timeout=10)
            if res.status_code == 200:
                data = res.json()
                msg = "\n".join([f"{k}: {v}" for k, v in data.items()])
                QMessageBox.information(self, "ViaCEP", msg)
                self.db.salvar_busca(cep, "ViaCEP")
            else:
                QMessageBox.warning(self, "Erro", "CEP não encontrado via ViaCEP")
        except Exception as e:
            QMessageBox.warning(self, "Erro", str(e))

    def exportar(self):
        # FIX: exporta direto do banco, sem iterar célula por célula
        import pandas as pd
        path, _ = QFileDialog.getSaveFileName(self, "Salvar CSV", "", "*.csv")
        if not path:
            return
        with get_conn(self.db_path) as conn:
            df = pd.read_sql_query(
                "SELECT mod_data,codigo_cliente,cep_original,cidade,rua,bairro,numero,estado,cnpj_cpf,nome_razao "
                "FROM ceps ORDER BY mod_data DESC",
                conn
            )
        df.to_csv(path, sep=";", index=False, encoding="utf-8-sig")
        QMessageBox.information(self, "Exportar", "Exportação concluída com sucesso!")

    def clean_column(self, col):
        import unicodedata
        col = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('ascii')
        col = col.lower()
        col = re.sub(r'[^a-z0-9]+', '_', col)
        return col.strip('_')

    def formatar_cep(self, cep):
        return re.sub(r'\D', '', str(cep or "")).zfill(8)


# ---------------- Executar standalone ----------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = App()
    window.setWindowTitle("Sistema de Consulta CEPs")
    window.resize(1200, 600)
    window.show()
    sys.exit(app.exec())