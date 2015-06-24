import pymongo
from bson.objectid import ObjectId

connection = pymongo.MongoClient()

db = connection["Attribute_Correction"]
CFVars = db["CFVars"]
KnownFixes = db["KnownFixes"]

# CF VARIABLES TABLE
#--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CFVars.insert({"Variable": "Surface Temperature (SST+Land)", "Var Name": "ts",       "CF Standard Name": "surface_temperature",                           "Units": "K"})
CFVars.insert({"Variable": "Mean sea level pressure",        "Var Name": "psl",      "CF Standard Name": "air_pressure_at_sea_level",                     "Units": "Pa"})
CFVars.insert({"Variable": "Convective precipitation",       "Var Name": "precc",    "CF Standard Name": "convective_precipitation_rate",                 "Units": "m s-1"})
CFVars.insert({"Variable": "Downward surface solar",         "Var Name": "rsds",     "CF Standard Name": "surface_downwelling_shortwave_flux_in_air",     "Units": "W m-2"})
CFVars.insert({"Variable": "Downward surface longwave",      "Var Name": "rlds",     "CF Standard Name": "surface_downwelling_longwave_flux_in_air",      "Units": "W m-2"})
CFVars.insert({"Variable": "Net surface solar",              "Var Name": "rss",      "CF Standard Name": "surface_net_downward_shortwave_flux",           "Units": "W m-2"})
CFVars.insert({"Variable": "Net surface longwave",           "Var Name": "rls",      "CF Standard Name": "surface_net_downward_longwave_flux",            "Units": "W m-2"})
CFVars.insert({"Variable": "Top net solar",                  "Var Name": "rst",      "CF Standard Name": "toa_net_downward_shortwave_flux",               "Units": "W m-2"})
CFVars.insert({"Variable": "Top net longwave",               "Var Name": "rlt",      "CF Standard Name": "toa_net_downward_longwave_flux",                "Units": "W m-2"})
CFVars.insert({"Variable": "Surface latent flux",            "Var Name": "hflsd",    "CF Standard Name": "surface_downward_latent_heat_flux",             "Units": "W m-2"})
CFVars.insert({"Variable": "Surface sensible flux",          "Var Name": "hfssd",    "CF Standard Name": "surface_downward_sensible_heat_flux",           "Units": "W m-2"})
CFVars.insert({"Variable": "Total cloud cover",              "Var Name": "clt",      "CF Standard Name": "cloud_area_fraction",                           "Units": "1"})
CFVars.insert({"Variable": "Geopotential",                   "Var Name": "g",        "CF Standard Name": "geopotential",                                  "Units": "m2 s-2"})
CFVars.insert({"Variable": "Temperature",                    "Var Name": "ta",       "CF Standard Name": "air_temperature",                               "Units": "K"})
CFVars.insert({"Variable": "Zonal velocity",                 "Var Name": "ua",       "CF Standard Name": "eastward_wind",                                 "Units": "m s-1"})
CFVars.insert({"Variable": "Specific humidity",              "Var Name": "hus",      "CF Standard Name": "specific_humidity",                             "Units": "1"})
CFVars.insert({"Variable": "Potential temperature",          "Var Name": "thetao",   "CF Standard Name": "sea_water_potential_temperature",               "Units": "K"})
CFVars.insert({"Variable": "Salinity",                       "Var Name": "so",       "CF Standard Name": "sea_water_salinity",                            "Units": "1e-3"})
CFVars.insert({"Variable": "Zonal velocity",                 "Var Name": "uo",       "CF Standard Name": "sea_water_x_velocity",                          "Units": "m s-1"})
CFVars.insert({"Variable": "Meridional velocity",            "Var Name": "vo",       "CF Standard Name": "sea_water_y_velocity",                          "Units": "m s-1"})
CFVars.insert({"Variable": "Vertical velocity",              "Var Name": "wo",       "CF Standard Name": "upward_sea_water_velocity",                     "Units": "m s-1"})
CFVars.insert({"Variable": "Sea level",                      "Var Name": "zoh",      "CF Standard Name": "sea_surface_height_above_geoid",                "Units": "m"})
CFVars.insert({"Variable": "Mixed layer depth",              "Var Name": "zmlo",     "CF Standard Name": "ocean_mixed_layer_thickness",                   "Units": "m"})
CFVars.insert({"Variable": "Sea-ice thickness",              "Var Name": "sit",      "CF Standard Name": "sea_ice_thickness",                             "Units": "m"})



CFVars.insert({"Variable": "2m T daily max",                 "Var Name": "tasmax",   "CF Standard Name": "air_temperature",                                "Units": "XXXXXXX"})
CFVars.insert({"Variable": "2m T daily min",                 "Var Name": "tasmin",   "CF Standard Name": "air_temperature",                                "Units": "XXXXXXX"})
CFVars.insert({"Variable": "Water equivalent snow depth",    "Var Name": "snowhlnd", "CF Standard Name": "water_equivalent_snow_depth",                    "Units": "XXXXXXX"})
CFVars.insert({"Variable": "Total soil moisture",            "Var Name": "mrsov",    "CF Standard Name": "volume_fraction_of_water_in_soil XXXXXX",        "Units": "1"})
CFVars.insert({"Variable": "Large scale precipitation",      "Var Name": "precl",    "CF Standard Name": "large_scale_precipitation XXXXXXX",              "Units": "XXXXXXX"})
CFVars.insert({"Variable": "Total runoff",                   "Var Name": "XXXXXXX",  "CF Standard Name": "total_runoff",                                   "Units": "XXXXXX"})
CFVars.insert({"Variable": "Sea-ice extent",                 "Var Name": "XXXXXXXX", "CF Standard Name": "sea_ice_extent",                                 "Units": "m2"})
CFVars.insert({"Variable": "Surface stress (x)",             "Var Name": "stx",      "CF Standard Name": "surface_zonal_stress_positive_to_the_west",      "Units": ""})
CFVars.insert({"Variable": "Surface stress (y)",             "Var Name": "sty",      "CF Standard Name": "surface_meridional_stress_positive_to_the_west", "Units": ""})
CFVars.insert({"Variable": "Precipitable water",             "Var Name": "tqm",      "CF Standard Name": "total_column_vertically_integrated_water",       "Units": ""})
CFVars.insert({"Variable": "2m temperature",                 "Var Name": "tas",      "CF Standard Name": "air_temperature",                                "Units": ""})

CFVars.insert({"Variable": "Meridional velocity",            "Var Name": "uv XX va", "CF Standard Name": "northward_wind",                                 "Units": "m s-1"})
CFVars.insert({"Variable": "10m wind (u)",                   "Var Name": "uas",      "CF Standard Name": "eastward_wind",                                  "Units": "XXXXXXXX"})
CFVars.insert({"Variable": "10m wind (v)",                   "Var Name": "vas",      "CF Standard Name": "northward_wind",                                 "Units": "XXXXXXXX"})



# KNOWN FIXES TABLE
#---------------------------------------------------------------------------------------------------------------------
KnownFixes.insert({"Incorrect Var": "air temp",       "Known Fix": "air_temperature",      "Times Seen": 1})
KnownFixes.insert({"Incorrect Var": "zonal velocity", "Known Fix": "sea_water_x_velocity", "Times Seen": 1})