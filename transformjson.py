import os
import pandas as pd

def transform():
    root_dir = os.path.dirname(os.getcwd())

    directories = [directory for directory in os.listdir(os.path.dirname(f"{root_dir}\\data\\"))]

    print(directories)

    customer_dir = root_dir + "\\data\\step_trainer"

    files = [filename for filename in os.listdir(customer_dir)]

    for file in files:
        try:
            pdf = pd.read_json(f"{customer_dir}\\{file}")

        except:
            print("Arquivo: ", file)

    for file in directories:
        with open(f"{root_dir}\\{file}", "r+") as customer_file:
            customers = customer_file.readlines()
            for customer in customers:
                customers = customer.replace("}{", "},{")
            customer_file.seek(0)
            customer_file.write("[")
            for customer in customers:
                customer_file.write(customer)
            customer_file.seek(0, 2)
            customer_file.write("]")