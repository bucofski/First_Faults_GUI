from business.core.plc_data import PlcMessage
from business.core.singleton import Singleton
from data.repositories.plc_repository import PLCRepository


class PlcService(metaclass=Singleton):

    def __init__(self):
        self.repo= PLCRepository()

    def get_plc_data(self):
        return

    def get_plc_last_32_data(self)->list[PlcMessage]:
        return self.repo.all_plc()[-32:]



