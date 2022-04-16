class Purchase:
    """units = capital / entry_unit
    risk_unit = entry_unit - stop_unit
    risk_value = units * risk_unit
    target_profit = PROFIT_TARGET * risk_unit
    target_price = entry_unit + target_profit
    """

    # 3 phases:
    # purchase - at the start of the lifeyle
    # partial - one or more sells have occurred
    # closure - when the last unit is sold

    def __init__(
        self,
        unit_quantity: float,
        unit_price: float,
        #        target_unit_price: float,
        #        stoploss_unit_price: float,
    ):
        self.units_sold = []
        self.units_held = []
        self.purchase_unit_quantity = unit_quantity
        self.purchase_unit_price = unit_price
        # self.purchase_target_unit_price = target_unit_price
        # self.purchase_target_total_value = unit_quantity * target_unit_price
        # self.purchase_stoploss_unit_price = stoploss_unit_price
        # self.purchase_stoploss_total_value = unit_quantity * stoploss_unit_price

        for u in range(0, unit_quantity):
            self.units_held.append(unit_price)
        self.units_bought = self.units_held

    def sell_units(self, sell_price: float, unit_quantity: float = None):
        if unit_quantity == None:
            unit_quantity = len(self.units_held)

        for u in range(0, unit_quantity):
            self.units_sold.append(sell_price)
            self.units_held.pop()

        print(f"Len of sold: {len(self.units_sold)}")
        print(f"Len of bought: {len(self.units_held)}")

    def get_average_sell_price(self):
        if len(self.units_sold) > 0:
            return sum(self.units_sold) / len(self.units_sold)
        else:
            return 0

    def get_profit(self):
        return sum(self.units_sold) - sum(self.units_bought[: len(self.units_sold)])


# b = Purchase(
#    unit_quantity=1000, unit_price=2, target_unit_price=3, stoploss_unit_price=1.5
# )
# b.sell_units(unit_quantity=5, sell_price=20)
# print("banana")
