import tkinter as tk
from tkinter import ttk

from view.benchmark_tab import BenchmarkTab
from view.execute_tab import ExecuteTab
from view.import_tab import ImportTab


class GUI:
    def __init__(self, model):
        self.model = model
        root = tk.Tk()
        root.title("Boardgames")
        root.geometry("800x600")

        tabs = ttk.Notebook(root)
        tabs.pack(fill=tk.BOTH, expand=True)

        self.execute_tab = ExecuteTab(tabs, self.model)
        self.benchmark_tab = BenchmarkTab(tabs, self.model)
        self.import_tab = ImportTab(tabs, self.model)

        tabs.add(self.execute_tab, text="Execute")
        tabs.add(self.benchmark_tab, text="Benchmark")
        tabs.add(self.import_tab, text="Import")

        root.mainloop()
