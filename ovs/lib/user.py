from ovs.dal.lists.userlist import UserList


class User(object):
    @staticmethod
    def get_user_by_username(username):
        return UserList.get_user_by_username(username)

    @staticmethod
    def get_users():
        return UserList.get_users()

