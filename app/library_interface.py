import mysql.connector


conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="yourpassword",
    database="library"
)
cursor = conn.cursor(dictionary=True)

def view_patron_profile_by_age(cursor, age_input):
    
    if 0 <= age_input <= 9:
        bin_label = '0 to 9 years'
    elif 10 <= age_input <= 19:
        bin_label = '10 to 19 years'
    elif 20 <= age_input <= 24:
        bin_label = '20 to 24 years'
    elif 25 <= age_input <= 34:
        bin_label = '25 to 34 years'
    elif 35 <= age_input <= 44:
        bin_label = '35 to 44 years'
    elif 45 <= age_input <= 54:
        bin_label = '45 to 54 years'
    elif 55 <= age_input <= 59:
        bin_label = '55 to 59 years'
    elif 60 <= age_input <= 64:
        bin_label = '60 to 64 years'
    elif 65 <= age_input <= 74:
        bin_label = '65 to 74 years'
    else:
        bin_label = '75 years and over'


    query = """
        SELECT 
            l.home_library_definition AS library,
            SUM(ds.total_checkouts) AS total_checkouts,
            SUM(ds.total_renewals) AS total_renewals,
            (SUM(ds.total_checkouts) + SUM(ds.total_renewals)) AS total_activity
        FROM demographic_summary ds
        JOIN library_info l ON ds.library_id = l.library_id
        WHERE ds.age_range = %s
        GROUP BY l.home_library_definition
        ORDER BY total_activity DESC;
    """

    cursor.execute(query, (bin_label,))
    results = cursor.fetchall()

    if results:
        print(f"\nLibrary activity for age group '{bin_label}':")
        print("-" * 80)
        print(f"{'Library':<25} {'Checkouts':>12} {'Renewals':>12} {'Total':>12}")
        print("-" * 80)
        for row in results:
            print(f"{row['library']:<25} {row['total_checkouts']:>12,} "
                  f"{row['total_renewals']:>12,} {row['total_activity']:>12,}")
        print("-" * 80)
        print(f"\nMost active library for this age group: {results[0]['library']} "
              f"({results[0]['total_activity']:,} total actions)\n")
    else:
        print(f"No data found for age group '{bin_label}'.")

    return results

def view_total_activity_all_libraries(cursor):
    query = """
        SELECT 
            l.home_library_definition AS library,
            SUM(p.total_checkouts) AS total_checkouts,
            SUM(p.total_renewals) AS total_renewals,
            (SUM(p.total_checkouts) + SUM(p.total_renewals)) AS total_activity
        FROM patrons p
        JOIN library_info l ON p.home_library_code = l.home_library_code
        GROUP BY l.home_library_definition
        ORDER BY total_activity DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    print("\nTotal activity across all libraries:\n")
    print(f"{'Library':<25} {'Checkouts':>15} {'Renewals':>15} {'Total Activity':>18}")
    print("-" * 73)
    for row in results:
        print(f"{row['library']:<25} {row['total_checkouts']:>15,} "
                f"{row['total_renewals']:>15,} {row['total_activity']:>18,}")
    print("-" * 73)
    print(f"Most active library: {results[0]['library']} "
            f"({results[0]['total_activity']:,} total actions)\n")
    return results


def total_checkouts_renewals(cursor):
    query = """
        SELECT 
            SUM(total_checkouts) AS total_checkouts,
            SUM(total_renewals) AS total_renewals,
            (SUM(total_checkouts) + SUM(total_renewals)) AS total_activity
        FROM patrons;
    """
    cursor.execute(query)
    result = cursor.fetchone()

    print("\nOverall Circulation Summary:")
    print("-" * 60)
    print(f"{'Total Checkouts:':<25} {result['total_checkouts']:>15,}")
    print(f"{'Total Renewals:':<25} {result['total_renewals']:>15,}")
    print(f"{'Total Activity:':<25} {result['total_activity']:>15,}")
    print("-" * 60)
    return result


def staff_summary(cursor):
    query = """
        SELECT 
            l.home_library_definition AS library,
            SUM(CASE WHEN st.patron_type_code IN (5, 15) THEN 1 ELSE 0 END) AS active_staff,
            SUM(CASE WHEN st.patron_type_code = 55 THEN 1 ELSE 0 END) AS retired_staff
        FROM staff_teachers st
        JOIN library_info l ON st.home_library_code = l.home_library_code
        GROUP BY l.home_library_definition
        ORDER BY active_staff DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    print("\nStaff Summary by Library:\n")
    print(f"{'Library':<25} {'Active Staff':>15} {'Retired Staff':>15}")
    print("-" * 58)
    for row in results:
        print(f"{row['library']:<25} {row['active_staff']:>15,} {row['retired_staff']:>15,}")
    print("-" * 58)
    print(f"Library with most active staff: {results[0]['library']} "
            f"({results[0]['active_staff']:,} active staff)\n")
    return results


def teachers_summary(cursor):
    query = """
        SELECT 
            l.home_library_definition AS library,
            COUNT(*) AS teacher_cards,
            SUM(st.total_checkouts) AS total_checkouts,
            SUM(st.total_renewals) AS total_renewals
        FROM staff_teachers st
        JOIN library_info l ON st.home_library_code = l.home_library_code
        WHERE st.patron_type_code = 15
        GROUP BY l.home_library_definition
        ORDER BY teacher_cards DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    total_teachers = sum(row['teacher_cards'] for row in results)
    print(f"\nTeacher Card Summary by Library:\n")
    print(f"{'Library':<25} {'Cards':>10} {'Checkouts':>15} {'Renewals':>15}")
    print("-" * 65)
    for row in results:
        print(f"{row['library']:<25} {row['teacher_cards']:>10,} {row['total_checkouts']:>15,} {row['total_renewals']:>15,}")
    print("-" * 65)
    print(f"{'Total':<25} {total_teachers:>10,}")
    print(f"\nLibrary with most teacher cards: {results[0]['library']} "
         f"({results[0]['teacher_cards']:,} cards)\n")
    return results


def digital_mail_cards_summary(cursor):
    query = """
        SELECT 
            l.home_library_definition AS library,
            SUM(CASE WHEN md.patron_type_code = 12 THEN 1 ELSE 0 END) AS digital_cards,
            SUM(CASE WHEN md.patron_type_code = 16 THEN 1 ELSE 0 END) AS mail_cards,
            SUM(md.total_checkouts) AS total_checkouts,
            SUM(md.total_renewals) AS total_renewals
        FROM library_mail_digital md
        JOIN patrons p ON md.patron_id = p.patron_id
        JOIN library_info l ON p.home_library_code = l.home_library_code
        GROUP BY l.home_library_definition
        ORDER BY (digital_cards + mail_cards) DESC;
    """
    cursor.execute(query)
    results = cursor.fetchall()

    total_digital = sum(row['digital_cards'] for row in results)
    total_mail = sum(row['mail_cards'] for row in results)

    print(f"\nDigital & Mail Card Summary by Library:\n")
    print(f"{'Library':<25} {'Digital':>10} {'Mail':>10} {'Checkouts':>15} {'Renewals':>15}")
    print("-" * 75)
    for row in results:
        print(f"{row['library']:<25} {row['digital_cards']:>10,} {row['mail_cards']:>10,} "
                f"{row['total_checkouts']:>15,} {row['total_renewals']:>15,}")
    print("-" * 75)
    print(f"{'Total':<25} {total_digital:>10,} {total_mail:>10,}")
    print(f"\nLibrary with most combined cards: {results[0]['library']} "
            f"({results[0]['digital_cards'] + results[0]['mail_cards']:,} total cards)\n")
    return results

def total_users_in_age_range(cursor, age_input):
    if 0 <= age_input <= 9:
        bin_label = '0 to 9 years'
    elif 10 <= age_input <= 19:
        bin_label = '10 to 19 years'
    elif 20 <= age_input <= 24:
        bin_label = '20 to 24 years'
    elif 25 <= age_input <= 34:
        bin_label = '25 to 34 years'
    elif 35 <= age_input <= 44:
        bin_label = '35 to 44 years'
    elif 45 <= age_input <= 54:
        bin_label = '45 to 54 years'
    elif 55 <= age_input <= 59:
        bin_label = '55 to 59 years'
    elif 60 <= age_input <= 64:
        bin_label = '60 to 64 years'
    elif 65 <= age_input <= 74:
        bin_label = '65 to 74 years'
    else:
        bin_label = '75 years and over'

    query = """
        SELECT 
            COUNT(*) AS total_users,
            SUM(CASE WHEN total_checkouts > 0 THEN 1 ELSE 0 END) AS repeat_patrons,
            SUM(CASE WHEN total_renewals > 0 THEN 1 ELSE 0 END) AS patrons_with_renewals
        FROM patrons
        WHERE age_range = %s;
    """
    cursor.execute(query, (bin_label,))
    result = cursor.fetchone()

    if result:
        print(f"\nAge group '{bin_label}':")
        print(f"• Total users: {result['total_users']:,}")
        print(f"• Repeat patrons: {result['repeat_patrons']:,}")
        print(f"• Patrons with renewals: {result['patrons_with_renewals']:,}")
    else:
        print(f"No users found for age group '{bin_label}'.")

    return result


def main_menu():
    while True:
        print("\n--- Library Analytics Menu ---")
        print("1. View Patron Profile by Age")
        print("2. Total Users in Age Range")
        print("3. Top Libraries")
        print("4. Total Checkouts and Renewals")
        print("5. Staff Summary")
        print("6. Teacher Cards Summary")
        print("7. Digital and Mail Cards Summary")
        print("8. Exit")
        choice = input("> ").strip()

        if choice == "1":
            age = input("Enter age: ").strip()
            if age.isdigit():
                view_patron_profile_by_age(cursor, int(age))
            else:
                print("Invalid age entered.")
        elif choice == "2":
            age = input("Enter age: ").strip()
            if age.isdigit():
                total_users_in_age_range(cursor, int(age))
            else:
                print("Invalid age entered.")
        elif choice == "3":
            view_total_activity_all_libraries(cursor)
        elif choice == "4":
            total_checkouts_renewals(cursor)
        elif choice == "5":
            staff_summary(cursor)
        elif choice == "6":
            teachers_summary(cursor)
        elif choice == "7":
            digital_mail_cards_summary(cursor)
        elif choice == "8":
            break
        else:
            print("Invalid choice, try again.")


if __name__ == "__main__":
    main_menu()
    cursor.close()
    conn.close()
