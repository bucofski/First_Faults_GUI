from data.repositories.plc_repository import PLCRepository


def test_data():

    repo=PLCRepository()
    print(repo.all_plc())


if __name__ == "__main__":
    test_data()