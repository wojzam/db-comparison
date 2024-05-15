import tkinter as tk
from tkinter import ttk

from model import Model, DATABASES_LABELS, QUERIES_LABELS


class ExecuteTab(tk.Frame):
    def __init__(self, master, model: Model, *args, **kwargs):
        super().__init__(master, *args, **kwargs)
        self.model = model

        self.database_var = tk.StringVar()
        self.database_var.set(DATABASES_LABELS[0])

        self.query_var = tk.StringVar()
        self.query_var.set(QUERIES_LABELS[0])

        frame_menu = tk.Frame(self)
        frame_menu.pack(pady=10)

        database_label = tk.Label(frame_menu, text="Select DB:")
        database_label.grid(row=0, column=0, sticky='w', padx=10, pady=10)

        frame_radio_buttons = tk.Frame(frame_menu)
        frame_radio_buttons.grid(row=0, column=1, sticky='ew', padx=10, pady=10)
        for label in DATABASES_LABELS:
            radiobutton = tk.Radiobutton(frame_radio_buttons, text=label, variable=self.database_var, value=label)
            radiobutton.pack(side="left")

        query_label = tk.Label(frame_menu, text="Select Query:")
        query_label.grid(row=1, column=0, sticky='w', padx=10, pady=10)
        query_menu = ttk.Combobox(frame_menu, textvariable=self.query_var, values=QUERIES_LABELS, width=45)
        query_menu.grid(row=1, column=1, sticky='ew', padx=10, pady=10)

        timer_label = tk.Label(frame_menu, text="Execution time:")
        timer_label.grid(row=2, column=0, sticky='w', padx=10, pady=10)
        self.timer_value_label = tk.Label(frame_menu)
        self.timer_value_label.grid(row=2, column=1, sticky='w', padx=10, pady=10)

        execute_button = tk.Button(self, text="Execute Query", command=self.execute_query)
        execute_button.pack(pady=20)

        frame_results = ttk.Panedwindow(self, orient=tk.HORIZONTAL)
        frame_results.pack(fill=tk.BOTH, expand=True)

        self.result_table = ttk.Treeview(frame_results, selectmode='browse')
        self.result_table['show'] = 'headings'
        frame_results.add(self.result_table, weight=1)

        vertical_scrollbar = ttk.Scrollbar(frame_results, orient=tk.VERTICAL, command=self.result_table.yview)
        frame_results.add(vertical_scrollbar, weight=0)

        horizontal_scrollbar = ttk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.result_table.xview)
        horizontal_scrollbar.pack(fill=tk.X, side=tk.BOTTOM)

        self.result_table.configure(xscrollcommand=horizontal_scrollbar.set, yscrollcommand=vertical_scrollbar.set)

    def execute_query(self):
        result, avg_time, std_time = self.model.execute_query(self.database_var.get(), self.query_var.get())
        self.show_result(result)
        self.timer_value_label.config(text='{:.4f} ± {:.4f}s'.format(avg_time, std_time))

    def show_result(self, result):
        self.result_table.delete(*self.result_table.get_children())
        self.result_table["columns"] = list(result.columns)
        for column in self.result_table["columns"]:
            self.result_table.heading(column, text=column)
        for index, row in result.iterrows():
            self.result_table.insert("", "end", values=list(row))
