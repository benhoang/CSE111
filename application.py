# random assignment for demo (customer integration)
import random 

# import sqlite3 for database access
import sqlite3
from sqlite3 import Error

# time for delay functionality 
import time

# connection to database 
database = r"tpch.sqlite"

def openConnection(_dbFile):
    print("++++++++++++++++++++++++++++++++++")
    print("Open database: ", _dbFile)

    conn = None
    try:
        conn = sqlite3.connect(_dbFile)
        print("success")
    except Error as e:
        print(e)

    print("++++++++++++++++++++++++++++++++++")

    return conn

def closeConnection(_conn, _dbFile):
    print("++++++++++++++++++++++++++++++++++")
    print("Close database: ", _dbFile)

    try:
        _conn.close()
        print("success")
    except Error as e:
        print(e)

    print("++++++++++++++++++++++++++++++++++")

def main_menu(): 
    print("1. Check Member Status")
    print("2. Make a New Trip")
    print("3. Modify Trip Details")
    print("4. Track a Train")
    print("5. Track a Bus")
    print("6. Exit. ")

conn = openConnection(database)


# Customer key list 
customer_list = [1,3,5,7,10,11,12,13,15,16,17,19,23,26,28,29,30,31] 

# print(len(customer_list))
customer_num = random.choice(customer_list)

print("\n")

print("You are now customer number: " + str(customer_num))

print("\n")

main_menu()

print("\n")

user_choice = input("Enter a number to continue: ")
print(user_choice)
print("\n")

# if member 
if str(user_choice) == "1" & str(customer_num) == "5" or "7" or "10" or "11" or "15" or "17" or "28": 
    print("You are currently a member of our rewards club. Choose the next option.")
    print("\n") 
    print("1. Cancel membership.") 
    print("2. Go back to previous menu.")
    print("\n")

    cust_ans = input("Choose what you would like to do next: ") 
    print(cust_ans)
    print("\n")

    ## ******** try to implement the random day in print statement, having errors with str and int combination in print statement 
    if cust_ans == "1": 
        print("If you would like to cancel your membership, you will lose access to your perks at the end of your billing cycle. Type [y/n] to continue")
        cancel_response = input("Enter y/n to continue: ") 
        if cancel_response == "y": 
            # rand_day = random.randint(0,30)
            print("We're sorry to see you go. Your perks will end on of this month.") 
            cancel2 = input("Would you like to do anything else? [y/n]")
            if cancel2 == "y": 
                main_menu() 
            else: 
                print("Goodbye now.")
                exit() 
else: 
    print("You are currently not a member. Would you like to join today? There is a monthly fee of $8.50.")
    non_member = input("Enter y/n to continue: ") 
    if non_member == "y": 
        print("You have succesfully join the rewards program! Sending you back to the main menu now.")
        time.sleep(2)
        main_menu()
    else: 
        print("Bringing you back to the main menu now.")
        time.sleep(2)
        main_menu()



closeConnection(conn, database)