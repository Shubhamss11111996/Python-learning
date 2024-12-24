class Employee:
    def __init__(self):
        print("I am in Constructor")

    def __del__(self):
        print("I am in Destructor")

# main program

eo1 = Employee()


