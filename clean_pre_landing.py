import os
import pandas as pd

def clean_json():
    root_dir = os.path.dirname("/Users/jimilburkhart/Programming/Python/WGU Code/D609 Udacity/")

    directories = [directory for directory in os.listdir(os.path.dirname(f"{root_dir}\\step_trainer\\landing"))]

    print(directories)

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

if __name__ == "__main__":
    clean_json()