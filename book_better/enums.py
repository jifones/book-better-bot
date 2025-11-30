import enum


class BetterVenue(str, enum.Enum):
    leytonstone = "leytonstone-leisure-centre"
    newham = "newham-leisure-centre"
    walthamstow = "walthamstow-leisure-centre"
    copper_box = "copper-box-arena"
    ISLINGTON_TENNIS_CENTRE = "islington-tennis-centre"
    islington_tennis_centre = "islington-tennis-centre"


class BetterActivity(str, enum.Enum):
    badminton_40_mins = "badminton-40min"
    HIGHBURY_TENNIS = "highbury-tennis"
