from pymongo import MongoClient
from mongo_library_setup import patrons_col, libraries_col




def get_age_bin(age_input):
    if 0 <= age_input <= 9:
        return '0 to 9 years'
    elif 10 <= age_input <= 19:
        return '10 to 19 years'
    elif 20 <= age_input <= 24:
        return '20 to 24 years'
    elif 25 <= age_input <= 34:
        return '25 to 34 years'
    elif 35 <= age_input <= 44:
        return '35 to 44 years'
    elif 45 <= age_input <= 54:
        return '45 to 54 years'
    elif 55 <= age_input <= 59:
        return '55 to 59 years'
    elif 60 <= age_input <= 64:
        return '60 to 64 years'
    elif 65 <= age_input <= 74:
        return '65 to 74 years'
    else:
        return '75 years and over'



def view_patron_profile_by_age(patrons_col, age_input):
    age_bin = get_age_bin(age_input)
    pipeline = [
        {"$match": {"age_range": age_bin}},
        {"$group":
         {
            "_id": "$library.name",
            "total_checkouts": {"$sum": "$total_checkouts"},
            "total_renewals": {"$sum": "$total_renewals"},
            "total_activity": {"$sum": {"$add": ["$total_checkouts", "$total_renewals"]}}
            }
         },
        {"$sort": {"total_activity": -1}}
    ]
    results = list(patrons_col.aggregate(pipeline))
    if results:
        print(f"\nLibrary activity for age group '{age_bin}':")
        print("-" * 80)
        print(f"{'Library':<25} {'Checkouts':>12} {'Renewals':>12} {'Total':>12}")
        print("-" * 80)
        for row in results:
            print(f"{row['_id']:<25} {row['total_checkouts']:>12,} "
                  f"{row['total_renewals']:>12,} {row['total_activity']:>12,}")
        print("-" * 80)
        print(f"Most active library for this age group: {results[0]['_id']} "
              f"({results[0]['total_activity']:,} total actions)\n")
    else:
        print(f"No data found for age group '{age_bin}'.")
    return results


def view_total_activity_all_libraries():
    pipeline = [
        {
            "$group":
            {
                "_id": "$library.name",
                "total_checkouts": {"$sum": "$total_checkouts"},
                "total_renewals": {"$sum": "$total_renewals"}
                }
            },
        {
            "$addFields": {
                "total_activity": {"$add": ["$total_checkouts", "$total_renewals"]}
                }
            },
        {
            "$sort": {"total_activity": -1}
        }
    ]

    results = list(patrons_col.aggregate(pipeline))

    print("\nTotal activity across all libraries:\n")
    print(f"{'Library':<25} {'Checkouts':>15} {'Renewals':>15} {'Total Activity':>18}")
    print("-" * 73)
    for row in results:
        print(f"{row['_id']:<25} {row['total_checkouts']:>15,} "
              f"{row['total_renewals']:>15,} {row['total_activity']:>18,}")
    print("-" * 73)
    print(f"Most active library: {results[0]['_id']} "
          f"({results[0]['total_activity']:,} total actions)\n")
    return results


def total_checkouts_renewals(patrons_col):
    pipeline = [
        {"$group":
         {
            "_id": None,
            "total_checkouts": {"$sum": "$total_checkouts"},
            "total_renewals": {"$sum": "$total_renewals"},
            "total_activity": {"$sum": {"$add": ["$total_checkouts", "$total_renewals"]}}
            }
         }
    ]
    result = list(patrons_col.aggregate(pipeline))[0]
    print("\nOverall Circulation Summary:")
    print("-" * 60)
    print(f"{'Total Checkouts:':<25} {result['total_checkouts']:>15,}")
    print(f"{'Total Renewals:':<25} {result['total_renewals']:>15,}")
    print(f"{'Total Activity:':<25} {result['total_activity']:>15,}")
    print("-" * 60)
    return result



def staff_summary(patrons_col):
    pipeline = [
        {"$match": {"patron_type_code": {"$in": [5, 15, 55]}}},
        {"$group":
         {
            "_id": {
                "code": "$library.code",
                "name": {"$ifNull": ["$library.name", "Unknown"]}
            },
            "active_staff": {"$sum": {"$cond": [{"$in": ["$patron_type_code", [5, 15]]}, 1, 0]}},
            "retired_staff": {"$sum": {"$cond": [{"$eq": ["$patron_type_code", 55]}, 1, 0]}}
            }
         },
        {"$sort": {"active_staff": -1}}
    ]

    results = list(patrons_col.aggregate(pipeline))

    if not results:
        print("No staff data found.")
        return []

    print("\nStaff Summary by Library:\n")
    print(f"{'Library':<25} {'Active Staff':>15} {'Retired Staff':>15}")
    print("-" * 58)
    for row in results:
        print(f"{row['_id']['name']:<25} {row['active_staff']:>15,} {row['retired_staff']:>15,}")
    print("-" * 58)

    top_library = results[0]
    print(f"Library with most active staff: {top_library['_id']['name']} "
          f"({top_library['active_staff']:,} active staff, {top_library['retired_staff']:,} retired staff)\n")

    return results



def digital_mail_cards_summary():
    pipeline = [
        {
            "$addFields": {
                "library_name": {"$ifNull": ["$library.name", "Unknown"]},
                "digital_cards": {"$cond": [{"$eq": ["$patron_type_code", 16]}, 1, 0]},
                "mail_cards": {"$cond": [{"$eq": ["$patron_type_code", 12]}, 1, 0]},
            }
        },
        {
            "$group":
            {
                "_id": "$library_name",
                "digital_cards": {"$sum": "$digital_cards"},
                "mail_cards": {"$sum": "$mail_cards"},
                "total_checkouts": {"$sum": "$total_checkouts"},
                "total_renewals": {"$sum": "$total_renewals"},
            }
        },
        {
            "$addFields": {"total_cards": {"$add": ["$digital_cards", "$mail_cards"]}}
        },
        {"$sort": {"total_cards": -1}}
    ]

    results = list(patrons_col.aggregate(pipeline))

    total_digital = sum(row['digital_cards'] for row in results)
    total_mail = sum(row['mail_cards'] for row in results)

    print(f"\nDigital & Mail Card Summary by Library:\n")
    print(f"{'Library':<25} {'Digital':>10} {'Mail':>10} {'Checkouts':>15} {'Renewals':>15}")
    print("-" * 75)
    for row in results:
        print(f"{row['_id']:<25} {row['digital_cards']:>10,} {row['mail_cards']:>10,} "
              f"{row['total_checkouts']:>15,} {row['total_renewals']:>15,}")
    print("-" * 75)
    print(f"{'Total':<25} {total_digital:>10,} {total_mail:>10,}")
    print(f"\nLibrary with most combined cards: {results[0]['_id']} "
          f"({results[0]['total_cards']:,} total cards)\n")

    return results


def teachers_summary(patrons_col):
    pipeline = [
        {"$match": {"patron_type_code": 15}},
        {"$group":
         {
            "_id": "$library.name",
            "teacher_cards": {"$sum": 1},
            "total_checkouts": {"$sum": "$total_checkouts"},
            "total_renewals": {"$sum": "$total_renewals"}
            }
         },
        {"$sort": {"teacher_cards": -1}}
    ]
    results = list(patrons_col.aggregate(pipeline))
    total_teachers = sum(row['teacher_cards'] for row in results)

    print(f"\nTeacher Card Summary by Library:\n")
    print(f"{'Library':<25} {'Cards':>10} {'Checkouts':>15} {'Renewals':>15}")
    print("-" * 65)
    for row in results:
        print(f"{row['_id']:<25} {row['teacher_cards']:>10,} "
              f"{row['total_checkouts']:>15,} {row['total_renewals']:>15,}")
    print("-" * 65)
    print(f"{'Total':<25} {total_teachers:>10,}")
    if results:
        print(f"\nLibrary with most teacher cards: {results[0]['_id']} "
              f"({results[0]['teacher_cards']:,} cards)\n")
    return results


def total_users_in_age_range(patrons_col, age_input):
    age_bin = get_age_bin(age_input)
    pipeline = [
        {"$match": {"age_range": age_bin}},
        {"$group":
         {
            "_id": None,
            "total_users": {"$sum": 1},
            "repeat_patrons": {"$sum": {"$cond": [{"$gt": ["$total_checkouts", 0]}, 1, 0]}},
            "patrons_with_renewals": {"$sum": {"$cond": [{"$gt": ["$total_renewals", 0]}, 1, 0]}}
            }
         }
    ]
    result = list(patrons_col.aggregate(pipeline))[0]
    print(f"\nAge group '{age_bin}':")
    print(f"• Total users: {result['total_users']:,}")
    print(f"• Repeat patrons: {result['repeat_patrons']:,}")
    print(f"• Patrons with renewals: {result['patrons_with_renewals']:,}")
    return result



def main_menu():
    while True:
        print("\n--- Library Analytics Menu ---")
        print("1. View Patron Profile by Age")
        print("2. Total Users in Age Range")
        print("3. Total Activity Across Libraries")
        print("4. Total Checkouts and Renewals")
        print("5. Staff Summary")
        print("6. Teacher Cards Summary")
        print("7. Digital and Mail Cards Summary")
        print("8. Exit")
        choice = input("> ").strip()

        if choice == "1":
            age = input("Enter age: ").strip()
            if age.isdigit():
                view_patron_profile_by_age(patrons_col, int(age))
            else:
                print("Invalid age entered.")
        elif choice == "2":
            age = input("Enter age: ").strip()
            if age.isdigit():
                total_users_in_age_range(patrons_col, int(age))
            else:
                print("Invalid age entered.")
        elif choice == "3":
            view_total_activity_all_libraries()
        elif choice == "4":
            total_checkouts_renewals(patrons_col)
        elif choice == "5":
            staff_summary(patrons_col)
        elif choice == "6":
            teachers_summary(patrons_col)
        elif choice == "7":
            digital_mail_cards_summary()
        elif choice == "8":
            break
        else:
            print("Invalid choice, try again.")


if __name__ == "__main__":
    main_menu()
    client.close()
