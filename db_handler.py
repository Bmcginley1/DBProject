from MARIADB_CREDS import DB_CONFIG
from mariadb import connect
from models.RentalHistory import RentalHistory
from models.Waitlist import Waitlist
from models.Item import Item
from models.Rental import Rental
from models.Customer import Customer
from datetime import date, timedelta


conn = connect(user=DB_CONFIG["username"], password=DB_CONFIG["password"], host=DB_CONFIG["host"],
               database=DB_CONFIG["database"], port=DB_CONFIG["port"])


cur = conn.cursor()


# Bryson
def add_item(new_item: Item = None):
    """
    new_item - An Item object containing a new item to be inserted into the DB in the item table.
        new_item and its attributes will never be None.
    """
    raise NotImplementedError("you must implement this function")


# Gabby
def add_customer(new_customer: Customer = None):
    """
    new_customer - A Customer object containing a new customer to be inserted into the DB in the customer table.
        new_customer and its attributes will never be None.
    """
    # generate address sk
    cur.execute("""
        SELECT MAX(ca_address_sk) FROM customer_address
    """)
    result = cur.fetchone()[0]
    if result is None:
        address_sk = 1
    else:
        address_sk = result + 1

    # generate customer sk
    cur.execute("""
        SELECT MAX(c_customer_sk) FROM customer
    """)
    result = cur.fetchone()[0]
    if result is None:
        cust_sk = 1
    else:
        cust_sk = cur.fetchone()[0] + 1

    # splits address into components
    str_num, temp = new_customer.address.split(" ", 1)
    str_name, temp = temp.split(", ", 1)
    city, temp = temp.split(", ", 1)
    state, zip_code = temp.split(" ")

    # insert new row into customer_address
    cur.execute("""
        INSERT INTO customer_address (ca_address_sk, ca_street_number, ca_street_name, ca_city, ca_state, ca_zip)
            VALUES
            (?, ?, ?, ?, ?, ?)
    """, (
        address_sk,
        str_num,
        str_name,
        city,
        state,
        zip_code
    ))

    # inserts into customer
    f_name, l_name = new_customer.name.split(" ", 1)
    cur.execute("""
        INSERT INTO customer (c_customer_sk, c_customer_id, c_first_name, c_last_name, c_email_address, c_current_addr_sk)
            VALUES
            (?, ?, ?, ?, ?, ?)
    """, (
        cust_sk,
        new_customer.customer_id,
        f_name,
        l_name,
        new_customer.email,
        address_sk
    ))


# Bryson
def edit_customer(original_customer_id: str = None, new_customer: Customer = None):
    """
    original_customer_id - A string containing the customer id for the customer to be edited.
    new_customer - A Customer object containing attributes to update. If an attribute is None, it should not be altered.
    """
    raise NotImplementedError("you must implement this function")


# Gabby
def rent_item(item_id: str = None, customer_id: str = None):
    """
    item_id - A string containing the Item ID for the item being rented.
    customer_id - A string containing the customer id of the customer renting the item.
    """
    today = date.today()
    due_date = today + timedelta(days=14)

    cur.execute("""
        INSERT INTO rental (item_id, customer_id, rental_date, due_date)
            VALUES
            (?, ?, ?, ?)
    """, (
        item_id,
        customer_id,
        today,
        due_date
    ))

# Bryson
def waitlist_customer(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's new place in line.
    """
    raise NotImplementedError("you must implement this function")

# Gabby
def update_waitlist(item_id: str = None):
    """
    Removes person at position 1 and shifts everyone else down by 1.
    """
    cur.execute("""
        DELETE FROM waitlist 
        WHERE place_in_line = 1 AND item_id = ?
    """, (item_id,))
    cur.execute("""
        UPDATE waitlist 
        SET place_in_line = place_in_line - 1 
        WHERE item_id = ?
    """, (item_id,))


# Bryson
def return_item(item_id: str = None, customer_id: str = None):
    """
    Moves a rental from rental to rental_history with return_date = today.
    """
    raise NotImplementedError("you must implement this function")


# Gabby
def grant_extension(item_id: str = None, customer_id: str = None):
    """
    Adds 14 days to the due_date.
    """
    cur.execute("""
        UPDATE rental
        SET due_date = DATE_ADD(due_date, INTERVAL 14 DAY)
        WHERE item_id = ? AND customer_id = ?
    """, (item_id, customer_id))

# Bryson
def get_filtered_items(filter_attributes: Item = None,
                       use_patterns: bool = False,
                       min_price: float = -1,
                       max_price: float = -1,
                       min_start_year: int = -1,
                       max_start_year: int = -1) -> list[Item]:
    """
    Returns a list of Item objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


# Gabby
def get_filtered_customers(filter_attributes: Customer = None, use_patterns: bool = False) -> list[Customer]:
    """
    Returns a list of Customer objects matching the filters.
    """

    # to store condition strings and variables
    cond = []
    vars = []

    # create query string to add to
    total_query = """
                SELECT 
                    c.c_customer_id, c.c_first_name, 
                    c.c_last_name, a.ca_street_number, 
                    a.ca_street_name, a.ca_city, 
                    a.ca_state, a.ca_zip, c.c_email_address
                FROM customer c JOIN customer_address a ON c.c_current_addr_sk = a.ca_address_sk
            """

    if filter_attributes is not None:

        # get filters from filter_attributes
        cust_id = filter_attributes.customer_id
        name = filter_attributes.name
        addr = filter_attributes.address
        email = filter_attributes.email

        # filter by customer id
        if cust_id is not None:
            vars.append(cust_id)
            if use_patterns:
                cond.append("c.c_customer_id LIKE ?")
            else:
                cond.append("c.c_customer_id = ?")

        # filter by name
        if name is not None:
            first, last = name.split(" ", 1)
            vars.append(first)
            vars.append(last)

            if use_patterns:
                cond.append("c.c_first_name LIKE ? AND c.c_last_name LIKE ?")
            else:
                cond.append("c.c_first_name = ? AND c.c_last_name = ?")

        # filter by email
        if email is not None:
            vars.append(email)
            if use_patterns:
                cond.append("c.c_email_address LIKE ?")
            else:
                cond.append("c.c_email_address = ?")

        # filter by address
        if addr is not None:
            str_num, temp = addr.split(" ", 1)
            str_name, temp = temp.split(", ", 1)
            city, temp = temp.split(", ", 1)
            state, zip_code = temp.split(" ")

            vars.append(str_num)
            vars.append(str_name)
            vars.append(city)
            vars.append(state)
            vars.append(zip_code)

            if use_patterns:
                cond.append("""
                        a.ca_street_number LIKE ? AND
                        a.ca_street_name LIKE ? AND
                        a.ca_city LIKE ? AND
                        a.ca_state LIKE ? AND
                        a.ca_zip LIKE ?
                    """)
            else:
                cond.append("""
                        a.ca_street_number = ? AND
                        a.ca_street_name = ? AND
                        a.ca_city = ? AND
                        a.ca_state = ? AND
                        a.ca_zip = ?
                    """)

    # add conditions to query string
    if cond:
        total_query += " WHERE "
        for i in range(len(cond)):
            total_query += cond[i]
            if i < len(cond) - 1:
                total_query += " AND "

    # execute query and store
    cur.execute(total_query, tuple(vars))
    results = cur.fetchall()

    customers = []

    # create customer objects with results from query
    for i in results:
        customer_id = i[0]
        first_name = i[1]
        last_name = i[2]
        street_num = i[3]
        street_name = i[4]
        city = i[5]
        state = i[6]
        zip_code = i[7]
        email = i[8]

        full_name = first_name + " " + last_name
        full_address = street_num + " " + street_name + ", " + city + ", " + state + " " + zip_code

        customers.append(Customer(customer_id, full_name, full_address, email))

    return customers


# Bryson
def get_filtered_rentals(filter_attributes: Rental = None,
                         min_rental_date: str = None,
                         max_rental_date: str = None,
                         min_due_date: str = None,
                         max_due_date: str = None) -> list[Rental]:
    """
    Returns a list of Rental objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


# Gabby
def get_filtered_rental_histories(filter_attributes: RentalHistory = None,
                                  min_rental_date: str = None,
                                  max_rental_date: str = None,
                                  min_due_date: str = None,
                                  max_due_date: str = None,
                                  min_return_date: str = None,
                                  max_return_date: str = None) -> list[RentalHistory]:
    """
    Returns a list of RentalHistory objects matching the filters.
    """

    # to store condition strings and variables
    cond = []
    vars = []

    # create query string to add to
    total_query = """
        SELECT 
            item_id,
            customer_id,
            rental_date,
            due_date,
            return_date
        FROM rental_history
    """

    if filter_attributes is not None:

        # get filters from filter_attributes
        item_id = filter_attributes.item_id
        customer_id = filter_attributes.customer_id
        rental_date = filter_attributes.rental_date
        due_date = filter_attributes.due_date
        return_date = filter_attributes.return_date

        # filter by item id
        if item_id is not None:
            vars.append(item_id)
            cond.append("item_id = ?")

        # filter by customer id
        if customer_id is not None:
            vars.append(customer_id)
            cond.append("customer_id = ?")

        # filter by rental date
        if rental_date is not None:
            vars.append(rental_date)
            cond.append("rental_date = ?")

        # filter by due date
        if due_date is not None:
            vars.append(due_date)
            cond.append("due_date = ?")

        # filter by return date
        if return_date is not None:
            vars.append(return_date)
            cond.append("return_date = ?")

    # range filters
    if min_rental_date is not None:
        vars.append(min_rental_date)
        cond.append("rental_date >= ?")

    if max_rental_date is not None:
        vars.append(max_rental_date)
        cond.append("rental_date <= ?")

    if min_due_date is not None:
        vars.append(min_due_date)
        cond.append("due_date >= ?")

    if max_due_date is not None:
        vars.append(max_due_date)
        cond.append("due_date <= ?")

    if min_return_date is not None:
        vars.append(min_return_date)
        cond.append("return_date >= ?")

    if max_return_date is not None:
        vars.append(max_return_date)
        cond.append("return_date <= ?")

    # add conditions to query string
    if cond:
        total_query += " WHERE "
        for i in range(len(cond)):
            total_query += cond[i]
            if i < len(cond) - 1:
                total_query += " AND "

    # execute query and store
    cur.execute(total_query, tuple(vars))
    results = cur.fetchall()

    rental_histories = []

    # create RentalHistory objects with results from query
    for i in results:
        item_id = i[0]
        customer_id = i[1]
        rental_date = str(i[2])
        due_date = str(i[3])
        return_date = str(i[4])

        rental_histories.append(RentalHistory(
            item_id,
            customer_id,
            rental_date,
            due_date,
            return_date
        ))

    return rental_histories


# Bryson
def get_filtered_waitlist(filter_attributes: Waitlist = None,
                          min_place_in_line: int = -1,
                          max_place_in_line: int = -1) -> list[Waitlist]:
    """
    Returns a list of Waitlist objects matching the filters.
    """
    raise NotImplementedError("you must implement this function")


# Gabby
def number_in_stock(item_id: str = None) -> int:
    """
    Returns num_owned - active rentals. Returns -1 if item doesn't exist.
    """
    cur.execute("""
        SELECT i_num_owned 
        FROM item
        WHERE i_item_id = ?
    """, (item_id,))

    check = cur.fetchone()

    if check is None:
        return -1

    num_owned = check[0]

    cur.execute("""
        SELECT COUNT(item_id)
        FROM rental
        WHERE item_id = ?
    """, (item_id,))

    active_rentals = cur.fetchone()[0]

    num_stock = num_owned - active_rentals
    return num_stock


# Bryson
def place_in_line(item_id: str = None, customer_id: str = None) -> int:
    """
    Returns the customer's place_in_line, or -1 if not on waitlist.
    """
    raise NotImplementedError("you must implement this function")


# Gabby
def line_length(item_id: str = None) -> int:
    """
    Returns how many people are on the waitlist for this item.
    """
    cur.execute("""
        SELECT COUNT(*)
        FROM waitlist
        WHERE item_id = ?
    """, (item_id, ))

    return cur.fetchone()[0]

def save_changes():
    """
    Commits all changes made to the db.
    """
    conn.commit()


def close_connection():
    """
    Closes the cursor and connection.
    """
    if cur:
        cur.close()
        print("Cursor closed.")
    if conn:
        conn.close()
        print("Connection closed.")

