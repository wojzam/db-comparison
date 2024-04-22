import tkinter as tk
from tkinter import ttk
import time

from queries import MySqlQuery, SqliteQuery, MongoDbQuery, RedisQuery, Query

MYSQL = "mysql"
SQLITE = "sqlite"
MONGODB = "mongodb"
REDIS = "redis"

DATABASES_LABELS = [MYSQL, SQLITE, MONGODB, REDIS]
QUERIES = [q.__name__ for q in [Query.select_from_games, Query.select_from_artists, Query.select_from_demand_game]]
QUERIES_LABELS = [q.replace("_", " ").upper() for q in QUERIES]
QUERIES_DICT = dict(zip(QUERIES_LABELS, QUERIES))


class GUI:
    databases = {MYSQL: MySqlQuery(), SQLITE: SqliteQuery(), MONGODB: MongoDbQuery(), REDIS: RedisQuery()}

    def __init__(self):
        root = tk.Tk()
        root.title("Boardgames")
        root.geometry("640x480")

        self.database_var = tk.StringVar()
        self.database_var.set(DATABASES_LABELS[0])

        self.query_var = tk.StringVar()
        self.query_var.set(QUERIES_LABELS[0])

        frame1 = tk.Frame(root)
        frame1.pack(pady=10)

        database_label = tk.Label(frame1, text="Select DB:")
        database_label.grid(row=0, column=0, sticky='w', padx=10, pady=10)
        database_menu = ttk.Combobox(frame1, textvariable=self.database_var, values=DATABASES_LABELS)
        database_menu.grid(row=0, column=1, sticky='ew', padx=10, pady=10)

        query_label = tk.Label(frame1, text="Select Query:")
        query_label.grid(row=1, column=0, sticky='w', padx=10, pady=10)
        query_menu = ttk.Combobox(frame1, textvariable=self.query_var, values=QUERIES_LABELS)
        query_menu.grid(row=1, column=1, sticky='ew', padx=10, pady=10)

        timer_label = tk.Label(frame1, text="Execute time:")
        timer_label.grid(row=2, column=0, sticky='w', padx=10, pady=10)
        self.timer_value_label = tk.Label(frame1)
        self.timer_value_label.grid(row=2, column=1, sticky='w', padx=10, pady=10)
        
        execute_button = tk.Button(root, text="Execute Query", command=self.execute_query)
        execute_button.pack(pady=20)

        frame2 = ttk.Panedwindow(root, orient=tk.HORIZONTAL)
        frame2.pack(fill=tk.BOTH, expand=True)

        self.result_table = ttk.Treeview(frame2, selectmode='browse')
        self.result_table['show'] = 'headings'
        frame2.add(self.result_table, weight=1)

        vertical_scrollbar = ttk.Scrollbar(frame2, orient=tk.VERTICAL, command=self.result_table.yview)
        frame2.add(vertical_scrollbar, weight=0)

        horizontal_scrollbar = ttk.Scrollbar(root, orient=tk.HORIZONTAL, command=self.result_table.xview)
        horizontal_scrollbar.pack(fill=tk.X, side=tk.BOTTOM)

        self.result_table.configure(xscrollcommand=horizontal_scrollbar.set, yscrollcommand=vertical_scrollbar.set)

        root.mainloop()

    def execute_query(self):
        start_time = time.time()
        db = self.databases[self.database_var.get()]
        query = QUERIES_DICT[self.query_var.get()]
        result = getattr(db, query)()
        self.show_result(result)
        end_time = time.time()
        self.timer_value_label.config(text = '{:.4f}'.format(end_time-start_time))

    def show_result(self, result):
        self.result_table.delete(*self.result_table.get_children())
        self.result_table["columns"] = list(result.columns)
        for column in self.result_table["columns"]:
            self.result_table.heading(column, text=column)
        for index, row in result.iterrows():
            self.result_table.insert("", "end", values=list(row))


if __name__ == "__main__":
    gui = GUI()
