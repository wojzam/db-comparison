import tkinter as tk
from tkinter import ttk

from analysis import DEFAULT_ITERATIONS, DEFAULT_STEP_COUNT
from model import Model, QUERIES_LABELS, DATABASES_LABELS
from view.input import IntInput

OPERATIONS = ["Read:", "Create", "Update", "Delete"]
MAX_ROWS = [10, 100, 1000, 5000, 10000, 50000]


class BenchmarkTab(tk.Frame):
    def __init__(self, master, model: Model, *args, **kwargs):
        super().__init__(master, *args, **kwargs)
        self.model = model

        self.database_vars = []

        self.operation_var = tk.StringVar()
        self.operation_var.set(OPERATIONS[0])

        self.query_var = tk.StringVar()
        self.query_var.set(QUERIES_LABELS[0])

        self.max_rows_var = tk.IntVar()
        self.max_rows_var.set(MAX_ROWS[2])

        frame_checkboxes = ttk.LabelFrame(self, text="Select Databases")
        frame_checkboxes.pack(padx=10, pady=10, fill=tk.X)

        for label in DATABASES_LABELS:
            var = tk.BooleanVar(value=True)
            self.database_vars.append(var)
            checkbox = tk.Checkbutton(frame_checkboxes, text=label, variable=var, onvalue=True, offvalue=False)
            checkbox.pack(padx=10, pady=10, side="left")

        frame_operation = ttk.LabelFrame(self, text="Select Operation")
        frame_operation.pack(padx=10, pady=10, fill=tk.X)

        query_menu = ttk.Combobox(frame_operation, textvariable=self.query_var, values=QUERIES_LABELS, width=50)
        query_menu.grid(row=0, column=1, sticky='w')

        for i, op in enumerate(OPERATIONS):
            radiobutton = tk.Radiobutton(frame_operation, text=op, variable=self.operation_var, value=op)
            radiobutton.grid(row=i, column=0, sticky='w', padx=10, pady=10)

        frame_settings = ttk.LabelFrame(self, text="Settings")
        frame_settings.pack(padx=10, pady=10, fill=tk.X)

        tk.Label(frame_settings, text="Test Iterations:").grid(row=0, column=0, sticky='w', padx=10, pady=10)
        self.test_iterations_input = IntInput(frame_settings, min_value=0, max_value=999,
                                              default_value=DEFAULT_ITERATIONS)
        self.test_iterations_input.grid(row=0, column=1, sticky='w', padx=10, pady=10)

        tk.Label(frame_settings, text="Step count:").grid(row=1, column=0, sticky='w', padx=10, pady=10)
        self.step_count_input = IntInput(frame_settings, min_value=0, max_value=99,
                                         default_value=DEFAULT_STEP_COUNT)
        self.step_count_input.grid(row=1, column=1, sticky='w', padx=10, pady=10)

        tk.Label(frame_settings, text="Max Rows Affected:").grid(row=2, column=0, sticky='w', padx=10, pady=10)
        frame_max_rows = ttk.Frame(frame_settings)
        frame_max_rows.grid(row=2, column=1, sticky='w', padx=0, pady=10)
        for rows in MAX_ROWS:
            radiobutton = tk.Radiobutton(frame_max_rows, text=str(rows), variable=self.max_rows_var, value=rows)
            radiobutton.pack(side=tk.LEFT, padx=10, pady=10)

        benchmark_button = tk.Button(self, text="Run Benchmark", command=self.benchmark_query)
        benchmark_button.pack(pady=20)

    def benchmark_query(self):
        match self.operation_var.get():
            case "Create":
                self.model.benchmark_create(*self.get_all_parameters())
            case "Update":
                self.model.benchmark_update(*self.get_all_parameters())
            case "Delete":
                self.model.benchmark_delete(*self.get_all_parameters())
            case _:
                self.model.benchmark_query(self.query_var.get(), *self.get_all_parameters())

    def get_all_parameters(self):
        included_db_labels = [d for v, d in zip(self.database_vars, DATABASES_LABELS) if v.get()]
        return (included_db_labels,
                self.max_rows_var.get(),
                self.test_iterations_input.get_value(),
                self.step_count_input.get_value())
