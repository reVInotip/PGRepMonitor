from view import IView
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                            QHBoxLayout, QLabel, QGraphicsView, QGraphicsScene, 
                            QGraphicsRectItem, QGraphicsLineItem, QGraphicsSimpleTextItem)
from PyQt5.QtCore import Qt, QTimer, QPointF
from PyQt5.QtGui import QColor, QBrush, QPen, QFont

class NodeView(QGraphicsRectItem):
    def __init__(self, name, x, y, width, height, end_lsn, start_lsn):
        super().__init__(0, 0, width, height)
        self.setPos(x, y)
        self.start_lsn = start_lsn
        self.end_lsn = end_lsn
        self.name = name
        self.progress_receive = 0
        self.progress_replay = 0
        self.setBrush(QBrush(Qt.lightGray))
        self.setPen(QPen(Qt.black, 2))
        
        # Текст с именем узла
        self.label = QGraphicsSimpleTextItem(name, self)
        self.label.setFont(QFont("Arial", 10))
        self.label.setPos(5, -25)
        
        # Прогресс-бар (растет вверх)
        self.receive_progress_bar = QGraphicsRectItem(0, height, width, 0, self)
        self.receive_progress_bar.setBrush(QBrush(QColor(100, 200, 150)))
        self.receive_progress_bar.setPen(QPen(Qt.NoPen))

        self.replay_progress_bar = QGraphicsRectItem(0, height, width, 0, self)
        self.replay_progress_bar.setBrush(QBrush(QColor(100, 200, 150)))
        self.replay_progress_bar.setPen(QPen(Qt.NoPen))
        
        # Текст с текущим значением
        self.value_text = QGraphicsSimpleTextItem("0", self)
        self.value_text.setFont(QFont("Arial", 8))
        self.value_text.setPos(width/2 - 10, height - 15)

    
    def set_progress(self, value):
        self.progress_receive = self.worker.calc_rec_diff(self.start_lsn)
        self.progress_replay = self.worker.calc_rec_diff(self.start_lsn)

        progress_height = self.rect().height() * (self.progress_receive / self.max_value)
        
        # Анимация роста вверх
        self.receive_progress_bar.setRect(
            0, 
            progress_height,
            self.rect().width(),
            progress_height
        )

        progress_height = self.rect().height() * (self.progress_replay / self.max_value)
        
        # Анимация роста вверх
        self.replay_progress_bar.setRect(
            0, 
            progress_height,
            self.rect().width(),
            progress_height
        )
        
        self.value_text.setText(f"{int(self.progress_receive)}%")
        self.value_text.setPos(
            self.rect().width()/2 - 10,
            self.rect().height() - 20
        )

        self.value_text.setText(f"{int(self.progress_replay)}%")
        self.value_text.setPos(
            self.rect().width()/2 - 10,
            self.rect().height() - 40
        )

class MasterNodeView(NodeView):
    def __init__(self, name, x, y, width, height, end_lsn, start_lsn, master):
        super().__init__(name, x, y, width, height, end_lsn, start_lsn)
        self.master = master
    
    def get_last_lsn(self):
        return self.master.get_current_lsn()

class ReplicationMonitor(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PostgreSQL Replication Monitor")
        self.setGeometry(100, 100, 800, 600)
        
        # Основной виджет
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        # Макет
        self.layout = QVBoxLayout(self.central_widget)
        
        # Графическая сцена
        self.view = QGraphicsView()
        self.scene = QGraphicsScene()
        self.view.setScene(self.scene)
        self.layout.addWidget(self.view)
        
        # Узлы и соединения
        self.nodes = {}
        self.connections = []
        
        # Настройка таймера для обновления
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_data)
        self.timer.start(1000)  # Обновление каждую секунду
        
        # Инициализация узлов
        self.init_nodes()
    
    def init_nodes(self):
        """Создание начальных узлов на сцене"""
        self.scene.clear()
        self.nodes = {}
        self.connections = []
        
        # Мастер-узел
        master = MasterNodeView("Master", 350, 100, 100, 300)
        self.scene.addItem(master)
        self.nodes["master"] = master
        
        # Реплики
        replicas = ["Replica1", "Replica2", "Replica3"]
        for i, name in enumerate(replicas):
            x_pos = 150 + i * 200
            node = NodeView(name, x_pos, 100, 100, 300)
            self.scene.addItem(node)
            self.nodes[name] = node
            
            # Соединение с мастером
            line = QGraphicsLineItem(
                master.rect().width()/2 + master.x(),
                master.rect().height() + master.y(),
                node.rect().width()/2 + node.x(),
                node.y()
            )
            line.setPen(QPen(Qt.darkGray, 2, Qt.DashLine))
            self.scene.addItem(line)
            self.connections.append(line)
    
    def update_data(self):
        """Обновление данных из PostgreSQL"""
        try:
            
            # Здесь должна быть реальная логика преобразования LSN в проценты
            # Для демонстрации используем случайные значения
            import random
            self.nodes["master"].set_progress(100)  # Мастер всегда на 100%
            
            for name in ["Replica1", "Replica2", "Replica3"]:
                if name in self.nodes:
                    # В реальном приложении замените на расчет из lag_bytes
                    progress = random.randint(70, 99)  # Случайное значение для демо
                    self.nodes[name].set_progress(progress)
            
        except Exception as e:
            print(f"Ошибка при обновлении данных: {e}")