class Memory:
    def __init__(self):
        self.state={"country":None,"zone_type":None}

    def update_from_spec(self, spec):
        if spec.filters.country: self.state["country"]=spec.filters.country
        if spec.filters.zone_type: self.state["zone_type"]=spec.filters.zone_type

    def get(self): return dict(self.state)
