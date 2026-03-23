import sys
import re
import os
import sqlite3
import pandas as pd
import webbrowser
import requests
from datetime import datetime
import subprocess

from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLineEdit,
    QFileDialog, QTableWidget, QTableWidgetItem, QMessageBox, QLabel, QProgressDialog, QHeaderView, QSlider
)
from PyQt6.QtGui import QKeySequence, QShortcut, QFont
from PyQt6.QtCore import QSettings, Qt

# ---------------- Utils ----------------
def get_db_path():
    pasta = os.path.join(os.path.expanduser("~"),"Desktop", "CONSULTORDECEP")
    os.makedirs(pasta, exist_ok=True)
    return os.path.join(pasta, "database.db")

# ---------------- Banco ----------------
class BancoCeps:
    def __init__(self, db_path):
        self.db_path = db_path
        self.criar_tabelas()

    def criar_tabelas(self):
        with sqlite3.connect(self.db_path) as conn:
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

    def upsert(self, df):
        cols = ["mod_data","codigo_cliente","cep_original","cep_tratado",
                "cidade","rua","bairro","numero","estado","cnpj_cpf","nome_razao"]

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

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(sql, dados)
            conn.commit()  

    def buscar_cep(self, cep):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute("""
                SELECT mod_data,codigo_cliente,cep_original,cidade,rua,
                       bairro,numero,estado,cnpj_cpf,nome_razao
                FROM ceps WHERE cep_tratado=?
            """, (cep,)).fetchall()

    def buscar_codigo(self, codigo):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute("""
                SELECT mod_data,codigo_cliente,cep_original,cidade,rua,
                       bairro,numero,estado,cnpj_cpf,nome_razao
                FROM ceps WHERE codigo_cliente=?
            """, (codigo,)).fetchall()

    def buscar_todos(self):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute("""
                SELECT mod_data,codigo_cliente,cep_original,cidade,rua,
                       bairro,numero,estado,cnpj_cpf,nome_razao
                FROM ceps ORDER BY mod_data DESC
            """).fetchall()

    def ultima_data(self):
        with sqlite3.connect(self.db_path) as conn:
            res = conn.execute("SELECT MAX(mod_data) FROM ceps").fetchone()
            return res[0] if res and res[0] else "Sem registros"

    def salvar_busca(self, cep, origem):
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT cep FROM historico_buscas ORDER BY id DESC LIMIT 1")
            last = cur.fetchone()
            if last and last[0] == cep:
                return
            cur.execute("""
                INSERT INTO historico_buscas (cep, origem, data_hora)
                VALUES (?, ?, ?)
            """, (cep, origem, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    def buscar_historico(self):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute("""
                SELECT cep, origem, data_hora
                FROM historico_buscas ORDER BY id DESC LIMIT 200
            """).fetchall()

    def top_ceps(self):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute("""
                SELECT cep, COUNT(*) FROM historico_buscas
                GROUP BY cep ORDER BY COUNT(*) DESC LIMIT 5
            """).fetchall()

# ---------------- App ----------------
class App(QWidget):
    def __init__(self):
        super().__init__()
        self.db_path = get_db_path()

        # Se o banco existe, conecta, se não, cria
        if not os.path.exists(self.db_path):
            open(self.db_path, "w").close()  # cria arquivo vazio
            QMessageBox.information(
        self,
        "Banco de Dados",
        f"O banco foi criado em:\n{self.db_path}"
    )

        self.db = BancoCeps(self.db_path)
        self.settings = QSettings("ConsultaCEP","App")
        self.setWindowTitle("Consulta de CEPs")
        self.setGeometry(100,100,1300,700)

        self.initUI()

        # Limpa qualquer dado antigo da tabela
        self.table.clearContents()
        self.table.setRowCount(0)
        self.table.setColumnCount(0)
        self.lbl_contador.setText("Registros: 0")
        self.lbl_ultima_data.setText(f"Última data: {self.db.ultima_data()}")  # novo label

        # Carrega dados do banco (vai estar vazio se banco recém-criado)
        self.carregar_dados()

    # ---------------- Interface ----------------
    def initUI(self):
        layout = QVBoxLayout()

        # Título e contador
        topo_label = QHBoxLayout()
        titulo = QLabel("📍 Sistema de Consultas ViaCEP / Correios")
        titulo.setFont(QFont("Segoe UI",20,QFont.Weight.Bold))
        titulo.setStyleSheet("color:#f0f0f0;")
        topo_label.addWidget(titulo)

        self.lbl_contador = QLabel("Registros: 0")
        self.lbl_contador.setFont(QFont("Segoe UI",12))
        self.lbl_contador.setStyleSheet("color:#f0f0f0; padding-left:20px;")
        topo_label.addWidget(self.lbl_contador)

        self.lbl_ultima_data = QLabel("Última data: -")
        self.lbl_ultima_data.setFont(QFont("Segoe UI",12))
        self.lbl_ultima_data.setStyleSheet("color:#f0f0f0; padding-left:20px;")
        topo_label.addWidget(self.lbl_ultima_data)

        topo_label.addStretch()
        layout.addLayout(topo_label)

        # Botões CSV, Exportar, Histórico, Estatísticas
        top = QHBoxLayout()
        self.btn_csv = self.criar_botao("📂 Importar CSV","#4e8cff","Importe um arquivo CSV com CEPs")
        self.btn_csv.clicked.connect(self.importar_csv)

        self.btn_export = self.criar_botao("💾 Exportar","#2ecc71","Exporte a tabela atual para CSV")
        self.btn_export.clicked.connect(self.exportar)

        self.btn_hist = self.criar_botao("🕒 Histórico","#9b59b6","Veja o histórico de buscas")
        self.btn_hist.clicked.connect(self.ver_historico)

        self.btn_stats = self.criar_botao("📊 Estatísticas","#e67e22","Veja os CEPs mais buscados")
        self.btn_stats.clicked.connect(self.ver_stats)

        self.btn_abrir_pasta = self.criar_botao("📂 Abrir Pasta do Banco","#3498db","Abrir a pasta onde o banco está salvo")
        self.btn_abrir_pasta.clicked.connect(self.abrir_pasta_banco)
        layout.addWidget(self.btn_abrir_pasta)

        for btn in [self.btn_csv,self.btn_export,self.btn_hist,self.btn_stats]:
            top.addWidget(btn)
        layout.addLayout(top)

        # Busca
        search = QHBoxLayout()
        self.input_cep = QLineEdit()
        self.input_cep.setPlaceholderText("Digite CEP(s) ou Código(s) (Coloque ';' para multiplas buscas)")
        self.input_cep.setStyleSheet(self.input_style())

        self.btn_buscar = self.criar_botao("🔎 Buscar","#1abc9c","Buscar CEP ou Código")
        self.btn_buscar.clicked.connect(self.buscar)

        self.btn_viacep = self.criar_botao("🌐 ViaCEP","#c0392b","Buscar CEP diretamente no ViaCEP")
        self.btn_viacep.clicked.connect(self.buscar_viacep)

        search.addWidget(self.input_cep)
        search.addWidget(self.btn_buscar)
        search.addWidget(self.btn_viacep)
        layout.addLayout(search)

        # Filtros
        filtro = QHBoxLayout()
        self.input_cidade = QLineEdit()
        self.input_cidade.setPlaceholderText("Cidade")
        self.input_cidade.setStyleSheet(self.input_style())

        self.input_estado = QLineEdit()
        self.input_estado.setPlaceholderText("Estado")
        self.input_estado.setStyleSheet(self.input_style())

        self.btn_filtrar = self.criar_botao("🔍 Filtrar","#2980b9","Filtrar CEPs por Cidade/Estado")
        self.btn_filtrar.clicked.connect(self.filtrar)

        self.btn_limpar = self.criar_botao("🧹 Limpar","#7f8c8d","Limpar filtros")
        self.btn_limpar.clicked.connect(self.limpar_filtro)

        filtro.addWidget(self.input_cidade)
        filtro.addWidget(self.input_estado)
        filtro.addWidget(self.btn_filtrar)
        filtro.addWidget(self.btn_limpar)
        layout.addLayout(filtro)

        # Tabela
        self.table = QTableWidget()
        self.table.setStyleSheet(self.table_style())
        self.table.setSortingEnabled(True)
        self.table.setAlternatingRowColors(True)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(False)

        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.table.setHorizontalScrollMode(QTableWidget.ScrollMode.ScrollPerPixel)  
        self.table.setSizeAdjustPolicy(QTableWidget.SizeAdjustPolicy.AdjustIgnored)
        self.table.setWordWrap(False)  

        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectItems)
        self.table.setSelectionMode(QTableWidget.SelectionMode.ExtendedSelection)
        self.table.horizontalHeader().setSectionsClickable(True)

        layout.addWidget(self.table)

        # Slider para alterar altura da tabela
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

        # Atalho Ctrl+C
        copy_shortcut = QShortcut(QKeySequence("Ctrl+C"),self.table)
        copy_shortcut.activated.connect(self.copiar_tabela)

        # Correios
        self.btn_correios = self.criar_botao("🏤 Abrir Correios","#f1c40f","Abrir site dos Correios")
        self.btn_correios.clicked.connect(self.correios)
        layout.addWidget(self.btn_correios)

        self.setLayout(layout)
        self.setStyleSheet("background-color:#1e1e2f;color:#f0f0f0;font-family:'Segoe UI';")

    # ---------------- Estilos ----------------
    def criar_botao(self,text,color,tooltip):
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

    def hover_color(self,color):
        import colorsys
        color=color.lstrip('#')
        r,g,b=tuple(int(color[i:i+2],16)/255.0 for i in (0,2,4))
        h,l,s=colorsys.rgb_to_hls(r,g,b)
        l=min(1.0,l+0.15)
        r,g,b=colorsys.hls_to_rgb(h,l,s)
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

    def table_style(self):
        return """
        QTableWidget {
            background-color:#2a2a3d;
            color:#f0f0f0;
            gridline-color:#444;
            font-size:14px;
        }
        QHeaderView::section {
            background-color:#252536;
            padding:5px;
            border:none;
            color:#f0f0f0;
        }
        QTableWidget::item:selected {
            background-color:#1abc9c;
            color:#fff;
        }
        QTableWidget::item:hover {
            background-color:#34495e;
        }"""

    # ---------------- Auxiliares ----------------
    def clean_column(self,col):
        col = col.lower().strip()
        col = re.sub(r'[áàãâä]','a',col)
        col = re.sub(r'[éèêë]','e',col)
        col = re.sub(r'[íìîï]','i',col)
        col = re.sub(r'[óòôõö]','o',col)
        col = re.sub(r'[úùûü]','u',col)
        col = re.sub(r'[ç]','c',col)
        col = re.sub(r'[^a-z0-9]','_',col)
        return col

    def formatar_cep(self,cep):
        return re.sub(r"\D","",cep).zfill(8)

    # ---------------- Limpeza e Preenchimento ----------------
    def preencher(self,dados):
        headers=["Data","Código","CEP","Cidade","Rua","Bairro","Número","Estado","CNPJ","Nome"]

        if self.table.columnCount() == 0:
            self.table.setColumnCount(len(headers))
            self.table.setHorizontalHeaderLabels(headers)

        self.table.setRowCount(0)

        for i,row in enumerate(dados):
            self.table.insertRow(i)
            for j,val in enumerate(row):
                self.table.setItem(i,j,QTableWidgetItem(str(val)))

        self.lbl_contador.setText(f"Registros: {len(dados)}")
        self.lbl_ultima_data.setText(f"Última data: {self.db.ultima_data()}")

        self.table.resizeColumnsToContents() 

    def carregar_dados(self):
        self.table.clear()
        self.table.setRowCount(0)
        self.table.setColumnCount(0)
        dados = self.db.buscar_todos()
        self.preencher(dados)

    # ---------------- Função copiar tabela ----------------
    def copiar_tabela(self):
        selecionados = self.table.selectedIndexes()
        if not selecionados: return
        selecionados = sorted(selecionados, key=lambda x:(x.row(), x.column()))
        texto = ""
        linha_atual = selecionados[0].row()
        for index in selecionados:
            if index.row() != linha_atual:
                texto += "\n"
                linha_atual = index.row()
            else:
                if texto and not texto.endswith("\n"):
                    texto += "\t"
            item = self.table.item(index.row(), index.column())
            texto += item.text() if item else ""
        QApplication.clipboard().setText(texto)

    # ---------------- Funções principais ----------------
    def importar_csv(self):
        path,_=QFileDialog.getOpenFileName(self,"CSV","","*.csv")
        if not path: return
        df=pd.read_csv(path,sep=";",dtype=str,encoding="utf-8-sig")
        df.columns=[self.clean_column(c) for c in df.columns]
        df.rename(columns={
            "modificacao__data":"mod_data",
            "codigo_cliente_c005":"codigo_cliente",
            "cep":"cep_original",
            "nome___razao_social":"nome_razao"
        },inplace=True)

        # --- Tratamento da coluna de data ---
        if "mod_data" in df.columns:
            df["mod_data"] = pd.to_datetime(df["mod_data"], errors="coerce", dayfirst=True)
            # Se alguma data não for válida, substitui pelo momento atual
            df["mod_data"] = df["mod_data"].fillna(pd.Timestamp.now())
            # Converte para string padronizada
            df["mod_data"] = df["mod_data"].dt.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # Se não existir coluna de data, cria com a data atual
            df["mod_data"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if "cep_original" not in df.columns:
            QMessageBox.critical(self,"Erro","Coluna CEP não encontrada!")
            return

        df["cep_tratado"]=df["cep_original"].fillna("").apply(self.formatar_cep)
        self.db.upsert(df)
        self.carregar_dados()

    def buscar(self):
        texto=self.input_cep.text()
        if not texto.strip():
            QMessageBox.warning(self,"Erro","Informe pelo menos um CEP ou Código")
            return
        valores=re.split(r"[\n;,]",texto)
        resultados=[]
        for v in valores:
            v=v.strip()
            if not v: continue
            if v.isdigit() and len(v)==8:  # CEP
                dados=self.db.buscar_cep(self.formatar_cep(v))
            else:  # Código
                dados=self.db.buscar_codigo(v)
            resultados.extend(dados)
        if resultados:
            self.preencher(resultados)
        else:
            self.preencher([])

    def buscar_viacep(self):
        ceps_raw=self.input_cep.text()
        if not ceps_raw.strip():
            QMessageBox.warning(self,"Erro","Informe pelo menos um CEP")
            return

        ceps=[self.formatar_cep(c) for c in re.split(r"[\n;,]",ceps_raw) if c.strip()]

        progress=QProgressDialog("Buscando no ViaCEP...",None,0,len(ceps),self)
        progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        progress.setAutoClose(True)

        session = requests.Session()  # 🔥 AQUI

        resultados=[]
        for i,cep in enumerate(ceps):
            progress.setValue(i)
            QApplication.processEvents()

            if len(cep)!=8: continue
            try:
                r=session.get(f"https://viacep.com.br/ws/{cep}/json/",timeout=5)  # 🔥 AQUI
                d=r.json()
                if "erro" in d: continue

                self.db.salvar_busca(cep,"VIACEP")

                resultados.append([("", "", d["cep"], d["localidade"],
                                d["logradouro"], d["bairro"], "",
                                d["uf"], "", "ViaCEP")])
            except Exception as e:
                print(f"Erro no CEP {cep}: {e}")
                continue

        progress.setValue(len(ceps))

        if resultados:
            dados_flat=[item for sublist in resultados for item in sublist]
            self.preencher(dados_flat)

    def filtrar(self):
        cidade=self.input_cidade.text().lower()
        estado=self.input_estado.text().lower()
        dados=self.db.buscar_todos()
        filtrados=[d for d in dados if cidade in (d[3] or "").lower() and estado in (d[7] or "").lower()]
        self.preencher(filtrados)

    def limpar_filtro(self):
        self.input_cidade.clear()
        self.input_estado.clear()
        self.carregar_dados()

    def exportar(self):
        path,_=QFileDialog.getSaveFileName(self,"Salvar","","*.csv")
        if not path: return
        data=[]
        for r in range(self.table.rowCount()):
            linha=[self.table.item(r,c).text() if self.table.item(r,c) else "" for c in range(self.table.columnCount())]
            data.append(linha)
        pd.DataFrame(data).to_csv(path,sep=";",index=False)

    def ver_historico(self):
        dados=self.db.buscar_historico()
        texto="\n".join([f"{d[0]} | {d[1]} | {d[2]}" for d in dados])
        QMessageBox.information(self,"Histórico",texto or "Sem dados")

    def ver_stats(self):
        dados=self.db.top_ceps()
        texto="\n".join([f"{c} → {t}x" for c,t in dados])
        QMessageBox.information(self,"Top CEPs",texto or "Sem dados")

    def correios(self):
        cep=self.formatar_cep(self.input_cep.text())
        QApplication.clipboard().setText(cep)
        webbrowser.open("https://buscacepinter.correios.com.br/app/endereco/index.php")

    def carregar_config(self):
        pass

    def abrir_pasta_banco(self):
        pasta = os.path.dirname(self.db_path)
        try:
            if sys.platform.startswith("win"):
                os.startfile(pasta)  # Windows
            elif sys.platform.startswith("darwin"):
                subprocess.Popen(["open", pasta])  # macOS
            else:
                subprocess.Popen(["xdg-open", pasta])  # Linux
        except Exception as e:
            QMessageBox.warning(self, "Erro", f"Não foi possível abrir a pasta:\n{e}")


if __name__=="__main__":
    app=QApplication(sys.argv)
    w=App()
    w.show()
    sys.exit(app.exec())