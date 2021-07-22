################## COUNTRY LIST #########################

def build_country_codes(filename):
    
    # string
    NOCOUNTRY_LIST = ""
    
    # file reader
    with open(filename, 'r') as reader:
        
        # save each line into an array
        lines_array = reader.readlines()

        for line in lines_array:

            NOCOUNTRY_LIST += ' -place_country:' + line.rstrip("\n")
    
    return NOCOUNTRY_LIST