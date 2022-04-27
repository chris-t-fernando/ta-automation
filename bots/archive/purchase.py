class Purchase:
    def __init__(
        self,
        unit_quantity: float,
        unit_price: float,
    ):
        self.units_sold = []
        self.units_held = []
        self.units_bought = []
        self.purchase_unit_quantity = unit_quantity
        self.purchase_unit_price = unit_price

        for u in range(0, unit_quantity):
            self.units_held.append(unit_price)
            self.units_bought.append(unit_price)

    def sell_units(self, sell_price: float, unit_quantity: float = None):
        if unit_quantity == None:
            unit_quantity = len(self.units_held)

        for u in range(0, unit_quantity):
            self.units_sold.append(sell_price)
            self.units_held.pop()

        return sell_price * unit_quantity

    def get_units(self):
        return len(self.units_held)

    def get_average_sell_price(self):
        if len(self.units_sold) > 0:
            return sum(self.units_sold) / len(self.units_sold)
        else:
            return 0

    def get_profit(self):
        return sum(self.units_sold) - sum(self.units_bought[: len(self.units_sold)])

    def get_returns(self):
        return sum(self.units_sold)

    def get_held_value(self, current_unit_price):
        return len(self.units_held) * current_unit_price


# b = Purchase(
#    unit_quantity=1000, unit_price=2, target_unit_price=3, stoploss_unit_price=1.5
# )
# b.sell_units(unit_quantity=5, sell_price=20)
# print("banana")
