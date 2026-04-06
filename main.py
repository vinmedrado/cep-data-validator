import sys
import os

from PyQt6.QtWidgets import QApplication, QMainWindow, QTabWidget
from PyQt6.QtCore import QTimer, Qt, QUrl
from PyQt6.QtWebEngineWidgets import QWebEngineView

from consulta_ceps import App as ConsultaCepsApp
from validacao import ValidacaoApp


# ---------------- Função correta para caminho ----------------
def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):  
        base_path = sys._MEIPASS
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))  

    return os.path.join(base_path, relative_path)


# ---------------- Main Window ----------------
class MainWindow(QTabWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Sistema de Consultas e Validações")

        self.setStyleSheet("""
            QTabWidget::pane {
                border:0;
                background-color:#1e1e2f;
            }
            QTabBar::tab {
                background:#2a2a3d;
                color:#f0f0f0;
                padding:8px 20px;
                border-top-left-radius:6px;
                border-top-right-radius:6px;
                margin-right:2px;
            }
            QTabBar::tab:selected {
                background:#1abc9c;
                color:#fff;
            }
            QTabBar::tab:hover {
                background:#34495e;
            }
        """)

        self.consulta = ConsultaCepsApp()
        self.addTab(self.consulta, "Consulta CEPs")

        self.validacao = ValidacaoApp()
        self.addTab(self.validacao, "Validação CPF/CNPJ")


# ---------------- Splash ----------------
class SplashWindow(QMainWindow):
    def __init__(self, callback):
        super().__init__()

        self.setWindowFlag(Qt.WindowType.FramelessWindowHint, True)
        self.setFixedSize(800, 500)

        # Centralizar
        screen = QApplication.primaryScreen().geometry()
        x = (screen.width() - self.width()) // 2
        y = (screen.height() - self.height()) // 2
        self.move(x, y)

        self.timer = QTimer()
        self.timer.timeout.connect(self.update_loading)

        self.progress = 0
        self.callback = callback

        # WebView
        self.view = QWebEngineView(self)
        self.setCentralWidget(self.view)

        html_path = resource_path("splash.html")

        # fallback pra evitar QUALQUER erro
        if os.path.exists(html_path):
            self.view.setUrl(QUrl.fromLocalFile(html_path))
        else:
            print("HTML não encontrado:", html_path)
            self.view.setHtml("<h1 style='color:white;background:black;'>Erro ao carregar splash</h1>")

        self.timer.start(50)

    def update_loading(self):
        self.progress += 1

        self.view.page().runJavaScript(f"updateProgress({self.progress});")

        if self.progress >= 100:
            self.timer.stop()
            self.close()
            self.callback()


# ---------------- Execução ----------------
if __name__ == "__main__":
    app = QApplication(sys.argv)

    def abrir_main():
        main_win = MainWindow()
        main_win.showMaximized()
        app.main_window = main_win

    splash = SplashWindow(abrir_main)
    splash.show()

    sys.exit(app.exec())