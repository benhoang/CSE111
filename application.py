# random assignment for demo (customer integration)
from queries import getCustKey
from queries import closeConnection
from queries import openConnection
from queries import printCity
from queries import getMemberCustKey
from queries import getmaxMemberCustKey
from queries import insertMember
from queries import deleteMember
from queries import insertOrder
from queries import showCustOrders
from queries import tripTrainDepart
from queries import tripBusDepart
from queries import getArrCity
from queries import cityName
from queries import showAllTrainStations
from queries import showTrainStations
from queries import trackTrainArrival
from queries import showAllBusStations
from queries import showBusStations
from queries import trackBusArrival
from queries import deleteOrder
from queries import showSeats
from queries import showPrefSeats
from queries import getSeats
from queries import getPrefSeats
from queries import updateTripSeat


import random

# import sqlite3 for database access
import sqlite3
from sqlite3 import Error

# time for delay functionality
import time



def main_menu():
    print("1. Check Member Status")
    print("2. Make a New Trip")
    print("3. Modify Trip Details")
    print("4. Track a Train")
    print("5. Track a Bus")
    print("6. Exit.\n")


def main(conn):
    while True:
        date_now = time.localtime(time.time())
        day = time.strftime("%Y-%m-%d", date_now)
        time_now = time.strftime('%H:%M', date_now)
        print("Today's date is: " + day)
        print("Today's time is: " + time_now)
        print("You are now customer number: " + str(customer_num) + "\n")
        main_menu()

        user_choice = input("Enter a number to continue: ")
        # print(user_choice)
        print("")

        
        member_list = getMemberCustKey(conn)
        
        isMem = False
        if customer_num in member_list:
            isMem = True

        # if Check Member Status 
        if user_choice == "1":
            if isMem:
                while True:
                    print("You are currently a member of our rewards club. Choose the next option.\n")
                    print("1. Cancel membership.")
                    print("2. Go back to previous menu.\n")

                    cust_ans = input("Choose what you would like to do next: ")
                    print("\n")
                    while True:
                        if cust_ans == "1":
                            print(
                                "If you would like to cancel your membership, you will lose access to your perks at the end of your billing cycle.\n")
                            cancel_response = input("Enter y/n to continue: ")
                            print("\n")
                            if cancel_response == "y":
                                # rand_day = random.randint(0,30)
                                deleteMember(conn, customer_num)
                                print("We're sorry to see you go. Your perks will end on of this month.\n")
                                cancel2 = input("Would you like to do anything else? [y/n] ")
                                
                                if cancel2 == "y":
                                    print("\n")
                                    pass
                                    return True
                                else:
                                    print("Goodbye now.\n")
                                    time.sleep(2)
                                    return False
                            elif cancel_response == "n":
                                print("\nBringing you back to the main menu now.\n")
                            else:
                                print("\nNot a valid command.\n")
                                continue
                        elif cust_ans == "2":
                            print("\nBringing you back to the main menu now.\n")
                            time.sleep(2)
                            break
                        else:
                            print("\nNot a valid command.\n")
                            break
                        break
                    break
            else:
                while True:
                    print("You are currently not a member. Would you like to join today? There is a monthly fee of $8.50.")
                    non_member = input("Enter y/n to continue: ")
                    if non_member == "y":
                        print("You have succesfully join the rewards program! Sending you back to the main menu now.\n")
                        newest_mem = getmaxMemberCustKey(conn)
                        insertMember(conn, customer_num, newest_mem[0] + 1)
                        time.sleep(2)
                        return True
                    elif non_member == "n":
                        print("\nBringing you back to the main menu now.\n")
                        time.sleep(2)
                        return True
                    else:
                        print("\nNot a valid command.\n")
                        continue

        # if Make a New Trip 
        elif user_choice == "2": 
            city_list = printCity(conn)
            for i in city_list: 
                print(i[0], i[1])
            print("\nHere is the list of available departure/destination locations.\n")
            
            # trip_depature is a integer 
            while True:
                trip_departure = input("Enter departure city: ") 
                try:
                    if int(trip_departure) >= 1 and int(trip_departure) <= 31:
                        td = tripTrainDepart(conn, trip_departure)
                        if trip_departure == '3':
                            td1 = tripBusDepart(conn, trip_departure)
                            for i in td1:
                                if i in td:
                                    pass
                                else:
                                    td.append(i)
                        elif trip_departure == '12':
                            td1 = tripBusDepart(conn, trip_departure)
                            for i in td1:
                                if i in td:
                                    pass
                                else:
                                    td.append(i)
                        elif trip_departure == '14':
                            td1 = tripBusDepart(conn, trip_departure)
                            for i in td1:
                                if i in td:
                                    pass
                                else:
                                    td.append(i)
                        elif trip_departure == '16':
                            td1 = tripBusDepart(conn, trip_departure)
                            for i in td1:
                                if i in td:
                                    pass
                                else:
                                    td.append(i)
                        elif trip_departure == '20':
                            td1 = tripBusDepart(conn, trip_departure)
                            for i in td1:
                                if i in td:
                                    pass
                                else:
                                    td.append(i)
                        elif trip_departure == '23':
                            td1 = tripBusDepart(conn, trip_departure)
                            for i in td1:
                                if i in td:
                                    pass
                                else:
                                    td.append(i)
                        elif trip_departure == '24':
                            td1 = tripBusDepart(conn, trip_departure)
                            for i in td1:
                                if i in td:
                                    pass
                                else:
                                    td.append(i)
                    # if station key is a train and bus line


                    # else if station key is a train line
                    

                    # else is a bus line
                    else:
                        td = tripBusDepart(conn, trip_departure)

                    print("")
                    for i in td: 
                        print(i[0], i[1])
                    d_city = cityName(conn, trip_departure)

                except IndexError or TypeError:
                    print("")
                    for i in city_list: 
                        print(i[0], i[1])
                    print("\nNot a valid entry. Choose another city.\n")
                    continue
                break

            # print("Here is the list of cities to go to. Enter")
            while True:
                a = getArrCity(conn, trip_departure)
                # print(a)
                
                trip_arrival = input("\nWhere would you like to go?: ") 
                try:
                    if int(trip_arrival) in a:
                        a_city = cityName(conn, trip_arrival)
                        break
                    else:
                        print("")
                        for i in td: 
                            print(i[0], i[1])
                        
                        print("\nNot a valid entry. Choose another city.")
                        continue
                except IndexError or TypeError or UnboundLocalError:
                    print("Error.\n")
                    continue


            print("\n\nOur trip is from " + d_city + " to " + a_city + "\n")

            
            conf = input("If this is correct, enter 1 to confirm, enter 2 to go back to menu: ")
            if conf == "1":
                while True:
                    try:
                        if isMem:
                            seats = getPrefSeats(conn, trip_departure)
                            if seats:
                                seat = random.choice(seats)
                            else:
                                seat = 0
                            print("Your total will be $7.50")
                            insertOrder(conn, customer_num, trip_departure, trip_arrival, seat[0], 7.50)
                            break
                        else:
                            seats = getSeats(conn, trip_departure)
                            if seats:
                                seat = random.choice(seats)
                            else:
                                seat = 0
                            print("Your total will be $15.00")
                            insertOrder(conn, customer_num, trip_departure, trip_arrival, seat[0], 15.00)
                            break
                    except ValueError or IndexError:
                        print("\nNot a valid seat.\n")
                        break
            if conf == "2": 
                print("Sending you back to main menu")
                return True

            # if trip_arrival 
            # insertOrder(conn,customer_num,seat)

        # if Modify Trip Details 
        elif user_choice == "3":
            while True:
                
                print("1. Cancel trip.\n") # delete trip 
                try:
                    mod_trip = input("Enter the number of what you would like to do: ")
                    
                    if int(mod_trip) == 1:
                        orderlist = showCustOrders(conn, customer_num)
                        if orderlist:
                            while True:
                                print("\n")
                                # prints the customers order 
                                j = 1
                                for i in orderlist:
                                    print(j,". ", i)
                                    j+=1
                                mod_trip = input("Choose the order you want to cancel: ")
                                try:
                                    order_key = orderlist[int(mod_trip)-1][0] 
                                    deleteOrder(conn, order_key)
                                    print("Your order has been cancelled.")
                                    print("Returning to menu now.")
                                    time.sleep(2)
                                    break
                                except IndexError:
                                    print("Not a valid order.\n")
                                    continue
                            break

                        else:
                            print("\nNo orders to cancel.\n")
                            time.sleep(2)
                            break
                    else: 
                        print("\nNot a valid command. Try again.\n")
                        time.sleep(2)
                        continue
                except ValueError:
                    print("\nNot a valid command. Try again.\n")
                    continue

        # if Track a Train 
        elif user_choice == "4": 
            while True:
                x = showAllTrainStations(conn)
                print("\nWhich train would you like to track? \n") 
                # ***** add an exception to make sure we enter a number ***** 
                try:
                    train_num = input("Enter the train number that you would like to track: ")
                    if int(train_num) in x:
                        while True:
                            print("")
                            y = showTrainStations(conn, train_num)
                            # determine which trainkey to look at 
                            try:
                                t_city = input("\nEnter the station number that you would like to track: ")
                                if int(t_city) in y:
                                    # connects to query to determine train departure 
                                    t_city1 = cityName(conn, t_city)
                                    # returns time 
                                    trackTrainArrival(conn, train_num, t_city1, time_now)
                                    time.sleep(2)
                                    print("Sending you back to the menu now")
                                    print("")
                                    time.sleep(2)
                                    break
                                else:
                                    print("\nNot a valid train station. Try again.")
                                    continue
                            except ValueError:
                                print("\nNot a valid train station. Try again.")
                                continue
                        break
                    else:
                        print("\nNot a valid train line. Try again.\n")
                        continue
                except ValueError:
                    print("\nNot a valid train line. Try again.\n")
                    continue


        # if Track a Bus 
        elif user_choice == "5":
            while True:
                x = showAllBusStations(conn)
                print("\nWhich bus would you like to track?\n")
                try:
                    bus_num = input("Enter the bus number that you would like to track: ") 
                    if int(bus_num) in x:
                        while True:
                            print("")
                            y = showBusStations(conn,bus_num)
                            # continue with bus here 
                            try:
                                b_city = input("\nEnter the bus station number that you would like to track: ")
                                if int(b_city) in y:
                                    # connects to query to determine bus departure 
                                    b_city1 = cityName(conn, b_city)
                                    # returns time 
                                    trackBusArrival(conn, bus_num, b_city1, time_now) 
                                    time.sleep(2)
                                    print("Sending you back to the menu now")
                                    print("")
                                    time.sleep(2)
                                    break
                                else:
                                    print("\nNot a valid bus station. Try again.")
                                    continue
                            except ValueError:
                                print("\nNot a valid bus station. Try again.")
                                continue
                        break
                    else:
                        print("\nNot a valid bus line. Try again.\n")
                        continue
                except ValueError:
                    print("\nNot a valid bus line. Try again.\n")
                    continue

        # if Exit. 
        elif user_choice == "6":
            print("Goodbye!\n")
            time.sleep(2)
            return False

        else:
            print("Not a valid command.\n")
            continue

run = True
database = r"tpch.sqlite"
# connection to database
conn = openConnection(database)

# Customer key list
# use query to get customers from database
customer_list = getCustKey(conn)

# print(len(customer_list))
customer_num = random.choice(customer_list)



while (run):
    # Retrieve current date 
    run = main(conn)

closeConnection(conn, database)