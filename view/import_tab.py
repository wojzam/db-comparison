import tkinter as tk
import webbrowser
from tkinter import ttk, filedialog

from model import DATABASES_LABELS


class ImportTab(tk.Frame):
    source_url = "https://www.kaggle.com/datasets/threnjen/board-games-database-from-boardgamegeek"

    def __init__(self, master, model, *args, **kwargs):
        super().__init__(master, *args, **kwargs)
        self.model = model

        self.source_directory = tk.StringVar()

        frame_transform = ttk.LabelFrame(self, text="Transform data")
        frame_transform.pack(padx=10, pady=10, fill=tk.X)
        ttk.Label(frame_transform, text="Download dataset from:", justify="left").grid(row=0, column=0, sticky='w',
                                                                                       padx=10, pady=10)
        link = tk.Label(frame_transform, text=self.source_url, foreground="blue",
                        font='TkDefaultFont 9 underline', justify="left", cursor="hand2")
        link.bind("<Button-1>", lambda e: open_url(self.source_url))
        link.grid(row=1, column=0, sticky='w', padx=10, pady=0)

        frame_directory = ttk.Frame(frame_transform)
        frame_directory.grid(row=2, column=0, sticky='w', padx=10, pady=15)
        ttk.Label(frame_directory, text="Source directory:").pack(side=tk.LEFT, padx=5)
        ttk.Entry(frame_directory, textvariable=self.source_directory, state="readonly").pack(side=tk.LEFT, padx=5)
        ttk.Button(frame_directory, text="Browse", command=self.browse_directory).pack(side=tk.LEFT, padx=5)
        ttk.Button(frame_directory, text="Transform", command=self.transform_data).pack(side=tk.LEFT, padx=5)

        frame_import = ttk.LabelFrame(self, text="Import")
        frame_import.pack(padx=10, pady=10, fill=tk.X)
        for db in DATABASES_LABELS:
            button_import = ttk.Button(frame_import, text=db, command=self.import_data(db))
            button_import.pack(padx=10, pady=15, side=tk.LEFT)

    def browse_directory(self):
        directory_path = filedialog.askdirectory()
        if directory_path:
            self.source_directory.set(directory_path)

    def transform_data(self):
        if self.source_directory.get():
            self.model.transform_data(self.source_directory.get())
        else:
            tk.messagebox.showerror(title="Error", message="Source directory is missing!")

    def import_data(self, db_label):
        def wrapped():
            self.model.import_data(db_label)

        return wrapped


def open_url(url):
    webbrowser.open_new(url)
