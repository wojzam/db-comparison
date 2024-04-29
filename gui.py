import tkinter as tk
from tkinter import ttk

from analysis import result_timed, show_time_comparison_plot
from queries import MySqlQuery, SqliteQuery, MongoDbQuery, RedisQuery, Query

MYSQL = "MySql"
SQLITE = "Sqlite"
MONGODB = "MongoDb"
REDIS = "Redis"

DATABASES_LABELS = [MYSQL, SQLITE, MONGODB, REDIS]
QUERIES = sorted([q for q in dir(Query) if callable(getattr(Query, q)) and not q.startswith("__")], key=len)
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
        query_menu = ttk.Combobox(frame1, textvariable=self.query_var, values=QUERIES_LABELS, width=30)
        query_menu.grid(row=1, column=1, sticky='ew', padx=10, pady=10)

        timer_label = tk.Label(frame1, text="Execution time:")
        timer_label.grid(row=2, column=0, sticky='w', padx=10, pady=10)
        self.timer_value_label = tk.Label(frame1)
        self.timer_value_label.grid(row=2, column=1, sticky='w', padx=10, pady=10)

        frame2 = tk.Frame(root)
        frame2.pack(fill=tk.Y, expand=True, pady=20)

        execute_button = tk.Button(frame2, text="Execute Query", command=self.execute_query)
        execute_button.pack(padx=5, side=tk.LEFT)

        benchmark_button = tk.Button(frame2, text="Benchmark Query", command=self.benchmark_query)
        benchmark_button.pack(padx=5, side=tk.RIGHT)

        frame3 = ttk.Panedwindow(root, orient=tk.HORIZONTAL)
        frame3.pack(fill=tk.BOTH, expand=True)

        self.result_table = ttk.Treeview(frame3, selectmode='browse')
        self.result_table['show'] = 'headings'
        frame3.add(self.result_table, weight=1)

        vertical_scrollbar = ttk.Scrollbar(frame3, orient=tk.VERTICAL, command=self.result_table.yview)
        frame3.add(vertical_scrollbar, weight=0)

        horizontal_scrollbar = ttk.Scrollbar(root, orient=tk.HORIZONTAL, command=self.result_table.xview)
        horizontal_scrollbar.pack(fill=tk.X, side=tk.BOTTOM)

        self.result_table.configure(xscrollcommand=horizontal_scrollbar.set, yscrollcommand=vertical_scrollbar.set)

        root.mainloop()

    def execute_query(self):
        db = self.databases[self.database_var.get()]
        query = QUERIES_DICT[self.query_var.get()]
        # TODO Move this logic to model
        result, avg_time, std_time = result_timed(getattr(db, query))
        self.show_result(result)
        self.timer_value_label.config(text='{:.4f} ± {:.4f}s'.format(avg_time, std_time))

    def benchmark_query(self):
        query = QUERIES_DICT[self.query_var.get()]
        # TODO Move this logic to model
        results = {name: result_timed(getattr(db, query), 20)[1:] for name, db in self.databases.items()}
        show_time_comparison_plot(self.query_var.get(), results)

    def show_result(self, result):
        self.result_table.delete(*self.result_table.get_children())
        self.result_table["columns"] = list(result.columns)
        for column in self.result_table["columns"]:
            self.result_table.heading(column, text=column)
        for index, row in result.iterrows():
            self.result_table.insert("", "end", values=list(row))


if __name__ == "__main__":
    gui = GUI()
