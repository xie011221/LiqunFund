class Car:
    def __init__(self,computer,engine):
        self.computer = computer
        self.engine = engine
        self.engine.set_car(self)
        self.gasoline = 1000

    def get_engine_power(self):
        return self.engine.get_power(self.gasoline)

    def get_theory_power(self):
        return self.computer.get_theory_power(self.gasoline)

class ElectricEngine:

    def set_car(self,car):
        self.car = car

    def get_power(self,gasoline):
        return gasoline * 0.15

    def get_gasoline(self):
        return self.car.gasoline

class BYDComputer:

    def get_theory_power(self,gasoline):
        return gasoline * 0.17


car = Car(BYDComputer(),ElectricEngine())

print('原有汽油：')
print(car.gasoline)
print('引擎动力：')
print(car.get_engine_power())
print('理论动力：')
print(car.get_theory_power())

print('引擎读取汽油：')
print(car.engine.get_gasoline())
print('修改后读取：')
car.gasoline = 73
print(car.engine.get_gasoline())