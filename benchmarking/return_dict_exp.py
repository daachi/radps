
#pip install pympler

if __name__ == "__main__":
    return_dict = {'cleanstate': "cleanstate",
        'cyclefactor': 0.99999,
        'cycleiterdone': 999,
        'cycleniter': 999,
        'cyclethreshold': 0.99999,
        'interactiveiterdone': 999,
        'interactivemode': False,
        'interactiveniter': 999,
        'interactivethreshold': 0.9999,
        'iterdone': 999,
        'loopgain': 0.999,
        'maxpsffraction': 0.999,
        'maxpsfsidelobe': 0.999,
        'minpsffraction': 0.999,
        'niter': 999,
        'nmajordone': 999,
        'nsigma': 0.9999,
        'stopcode': 9,
        }
    
    number_of_cycles = 10
    number_fields = 1
    number_of_channels = 1
    number_of_stokes = 1
    
    summarymajor = [9]*number_of_cycles
    
    summaryminor = {}
    
    for i_f in range(number_fields):
        summaryminor[i_f] = {}
        for i_c in range(number_of_channels):
            summaryminor[i_f][i_c] = {}
            for i_s in range(number_of_stokes):
                summaryminor[i_f][i_c][i_s] = {
                        "iterdone" : [999]*number_of_cycles,
                        "peakres" : [0.999]*number_of_cycles,
                        "modelflux" : [0.999]*number_of_cycles,
                        "cyclethres" : [0.999]*number_of_cycles,
                        "cyclestartiters" : [999]*number_of_cycles,
                        "startitersdone" : [999]*number_of_cycles,
                        "startpeakres" : [0.999]*number_of_cycles,
                        "startmodelflux" : [0.999]*number_of_cycles,
                        "startmodelflux" : [0.999]*number_of_cycles,
                        "startpeakres" : [0.999]*number_of_cycles,
                        "startmodelflux" : [0.999]*number_of_cycles,
                        "startpeakresnm" : [0.999]*number_of_cycles,
                        "peakresnm" : [0.999]*number_of_cycles,
                        "masksum" : [999]*number_of_cycles,
                        "mpiserver" : ["mpiserver"]*number_of_cycles,
                        "stopcode" : [9]*number_of_cycles
                }
                
            
    return_dict['summarymajor'] = summarymajor
    return_dict['summaryminor'] = summaryminor
    
    from pympler.asizeof import asizeof #need for get the size of the dict since it is a nested dict
    
    size = asizeof(return_dict) #size in bytes
    print(f"Size of the dict is {size/(1024 * 1024)} MB")
    
    