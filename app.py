import tkinter
import tkinter.messagebox
import customtkinter
from postgres_manager import PostgresManager
from mongo_manager import MongoManager
import matplotlib.pyplot as plt

customtkinter.set_appearance_mode("dark")  # Modes: "System" (standard), "Dark", "Light"
customtkinter.set_default_color_theme("blue")  # Themes: "blue" (standard), "green", "dark-blue"


class App(customtkinter.CTk):
    # configuration = Configuration()
    postgres_manager = PostgresManager()
    mongo_manager = MongoManager()

    def __init__(self):
        super().__init__()

        self.title("Database performance test")
        self.geometry(f"{600}x{400}")

        self.grid_columnconfigure((0, 1), weight=1)
        self.grid_rowconfigure(0, weight=1)

        # insert
        self.frame = customtkinter.CTkFrame(self)
        self.frame.grid(row=0, column=0, sticky="nesw")
        self.insert_size_label = customtkinter.CTkLabel(master=self.frame, text="Insert")
        self.insert_size_label.grid(row=0, column=1, padx=10, pady=5)
        self.insert_size_var = tkinter.StringVar(value='0')
        self.insert_size_entry = customtkinter.CTkEntry(self.frame, textvariable=self.insert_size_var)
        self.insert_size_entry.grid(row=0, column=2, padx=10, pady=5)
        self.insert_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_insert)
        self.insert_button.grid(row=0, column=3, padx=10, pady=5)
        self.insert_plot_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Plot",
                                                    command=self.run_insert_plot)
        self.insert_plot_button.grid(row=0, column=4, padx=10, pady=5)


        # select
        self.select_size_label = customtkinter.CTkLabel(master=self.frame, text="Select")
        self.select_size_label.grid(row=1, column=1, padx=10, pady=5)
        self.select_size_var = tkinter.StringVar(value='0')
        self.select_size_entry = customtkinter.CTkEntry(self.frame, textvariable=self.select_size_var)
        self.select_size_entry.grid(row=1, column=2, padx=10, pady=5)
        self.select_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_select)
        self.select_button.grid(row=1, column=3, padx=10, pady=5)
        self.select_plot_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Plot",
                                                    command=self.run_select_plot)
        self.select_plot_button.grid(row=1, column=4, padx=10, pady=5)

        # update
        self.update_size_label = customtkinter.CTkLabel(master=self.frame, text="Update")
        self.update_size_label.grid(row=2, column=1, padx=10, pady=5)
        self.update_size_var = tkinter.StringVar(value='0')
        self.update_size_entry = customtkinter.CTkEntry(self.frame, textvariable=self.update_size_var)
        self.update_size_entry.grid(row=2, column=2, padx=10, pady=5)
        self.update_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_update)
        self.update_button.grid(row=2, column=3, padx=10, pady=5)
        self.update_plot_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Plot",
                                                    command=self.run_update_plot)
        self.update_plot_button.grid(row=2, column=4, padx=10, pady=5)


        # delete
        self.delete_size_label = customtkinter.CTkLabel(master=self.frame, text="Delete")
        self.delete_size_label.grid(row=3, column=1, padx=10, pady=5)
        self.delete_size_var = tkinter.StringVar(value='0')
        self.delete_size_entry = customtkinter.CTkEntry(self.frame, textvariable=self.delete_size_var)
        self.delete_size_entry.grid(row=3, column=2, padx=10, pady=5)
        self.delete_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_delete)
        self.delete_button.grid(row=3, column=3, padx=10, pady=5)

        self.delete_plot_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Plot",
                                                    command=self.run_delete_plot)
        self.delete_plot_button.grid(row=3, column=4, padx=10, pady=5)


        # avg
        self.avg_size_label = customtkinter.CTkLabel(master=self.frame, text="Avg")
        self.avg_size_label.grid(row=4, column=1, padx=10, pady=5)
        self.avg_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_avg)
        self.avg_button.grid(row=4, column=2, padx=10, pady=5)

        # median
        self.median_size_label = customtkinter.CTkLabel(master=self.frame, text="Median")
        self.median_size_label.grid(row=5, column=1, padx=10, pady=5)
        self.median_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_median)
        self.median_button.grid(row=5, column=2, padx=10, pady=5)

        # count all
        self.count_all_size_label = customtkinter.CTkLabel(master=self.frame, text="Count all")
        self.count_all_size_label.grid(row=6, column=1, padx=10, pady=5)
        self.count_all_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_count_all)
        self.count_all_button.grid(row=6, column=2, padx=10, pady=5)
        self.results_label = customtkinter.CTkLabel(master=self.frame, text="Results")
        self.results_label.grid(row=6, column=4, padx=20, pady=5)


        # count by word
        self.count_by_word_size_label = customtkinter.CTkLabel(master=self.frame, text="Count by word")
        self.count_by_word_size_label.grid(row=7, column=1, padx=10, pady=5)
        self.count_by_word_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_count_by_world)
        self.count_by_word_button.grid(row=7, column=2, padx=10, pady=5)
        self.postgres_label = customtkinter.CTkLabel(master=self.frame, text="Postgres: ")
        self.postgres_label.grid(row=7, column=3, padx=20, pady=5)
        self.postgres_textbox = customtkinter.CTkTextbox(self.frame, width=150, height=20)
        self.postgres_textbox.grid(row=7, column=4, padx=10, pady=5)


        # min
        self.min_size_label = customtkinter.CTkLabel(master=self.frame, text="Min value")
        self.min_size_label.grid(row=8, column=1, padx=10, pady=5)
        self.min_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_min)
        self.min_button.grid(row=8, column=2, padx=10, pady=5)
        self.mongo_label = customtkinter.CTkLabel(master=self.frame, text="MongoDb: ")
        self.mongo_label.grid(row=8, column=3, padx=20, pady=5)
        self.mongo_textbox = customtkinter.CTkTextbox(self.frame, width=150, height=20)
        self.mongo_textbox.grid(row=8, column=4, padx=10, pady=5)


        # max
        self.max_size_label = customtkinter.CTkLabel(master=self.frame, text="Max value")
        self.max_size_label.grid(row=9, column=1, padx=10, pady=5)
        self.max_button = customtkinter.CTkButton(master=self.frame, fg_color="transparent",
                                                    border_width=2, text_color=("gray10", "#DCE4EE"), text="Test",
                                                    command=self.run_max)
        self.max_button.grid(row=9, column=2, padx=10, pady=5)

        self.cassandra_label = customtkinter.CTkLabel(master=self.frame, text="Cassandra: ")
        self.cassandra_label.grid(row=9, column=3, padx=20, pady=5)
        self.cassandra_textbox = customtkinter.CTkTextbox(self.frame, width=150, height=20)
        self.cassandra_textbox.grid(row=9, column=4, padx=10, pady=5)


    def run_insert(self):
        # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.insert(int(self.insert_size_var.get()))))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.insert(int(self.insert_size_var.get()))))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_insert_plot(self):
        # todo
        postgres = [self.postgres_manager.insert(i) for i in [1, 5, 10]]
        mongo = [self.mongo_manager.insert(i) for i in [1, 5, 10]]
        cassandra = [1, 5, 10]
        self.generate_plot("InsertExecutionTime.png", [1, 5, 10], postgres, cassandra, mongo)

    def run_select(self):
        # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.select(int(self.select_size_var.get()))))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.select(int(self.select_size_var.get()))))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_select_plot(self):
        # todo
        postgres = [self.postgres_manager.select(i) for i in [1, 5, 10]]
        mongo = [self.mongo_manager.select(i) for i in [1, 5, 10]]
        cassandra = [1, 5, 10]
        self.generate_plot("SelectExecutionTime.png", [1, 5, 10], postgres, cassandra, mongo)

    def run_update(self):
        # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.update(int(self.update_size_var.get()))))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.update(int(self.update_size_var.get()))))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_update_plot(self):
        # todo
        postgres = [self.postgres_manager.update(i) for i in [1, 5, 10]]
        mongo = [self.mongo_manager.update(i) for i in [1, 5, 10]]
        cassandra = [1, 5, 10]
        self.generate_plot("UpdateExecutionTime.png", [1, 5, 10], postgres, cassandra, mongo)

    def run_delete(self):
        # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.delete(int(self.delete_size_var.get()))))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.delete(int(self.delete_size_var.get()))))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_delete_plot(self):
        # todo
        postgres = [self.postgres_manager.delete(i) for i in [1, 5, 10]]
        mongo = [self.mongo_manager.delete(i) for i in [1, 5, 10]]
        cassandra = [1, 5, 10]
        self.generate_plot("DeleteExecutionTime.png", [1, 5, 10], postgres, cassandra, mongo)

    def run_avg(self):
        # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.avg()))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.avg()))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_median(self):
        # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.median()))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.median()))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_count_all(self):
         # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.count()))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.count()))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_count_by_world(self):
         # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.count_by_word()))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.count_by_word()))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_min(self):
         # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.min()))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.min()))
        self.cassandra_textbox.insert('3.0', '0.0')

    def run_max(self):
         # todo
        self.postgres_textbox.insert('1.0', text=str(self.postgres_manager.max()))
        self.mongo_textbox.insert('2.0', text=str(self.mongo_manager.max()))
        self.cassandra_textbox.insert('3.0', '0.0')

    def generate_plot(self, filename, x, postgres, cassandra, mongo):
        print(postgres)
        plt.plot(x, postgres, label='Postgres')
        plt.plot(x, cassandra, label='Cassandra')
        plt.plot(x, mongo, label='MongoDB')

        plt.title('filename')
        plt.xlabel('Number of rows')
        plt.ylabel('Execution time')
        plt.legend()
        
        plt.savefig(filename)


if __name__ == "__main__":
    app = App()
    app.mainloop()
