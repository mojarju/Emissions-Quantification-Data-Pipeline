import pandas as pd
import numpy


#Class used to perform emissions calculations
class ConOilnGasEmissionCalc:
    def __init__(self, emissions_df): 
        self.emissions_df = emissions_df
        self.co2_gwp = 1    #CO2 GWP update according to assessment record
        self.ch4_gwp = 28   #CH4 GWP update according to assessment record
        self.n2o_gwp = 265  #N2O GWP update according to assessment record


    #Calculating emissions from fuel consumption and flaring 
    def calculate_emissions(self, fuel_type, volume_col, co2_ef, ch4_ef, n2o_ef, co2_result, ch4_result, n2o_result, total_emission) -> float:
        """
        Calculation Equation: 
                Emissions[CO2, CH4, N2O](tonne) = FuelConsumption (e3m3) x ConversionFactor(1000m3/1e3m3) x EmissionFactor (tCO2e/m3)

            Default Emission Factors (Check Fuel/Flare volumetrics DataFrame/CSV files)
            Carbon Dioxide -> 0.00233 tonne/m3
            Methane -> 6.4E-6 tonne/m3
            NitrousOxide -> 6.0E-8 tonnes/m3 
        """
        # Calculate CO2 Emissions for 'FUEL' and 'FLARE' Activity types
        self.emissions_df[co2_result] = numpy.where(
            self.emissions_df[fuel_type] == 'FUEL', self.emissions_df[volume_col] * 1000 * self.emissions_df[co2_ef],
            numpy.where(self.emissions_df[fuel_type] == 'FLARE', 
                     self.emissions_df[volume_col] * 1000 * self.emissions_df[co2_ef] * 0.000001, 0)
            )
        
        # Calculate CH4 Emissions for 'FUEL' and 'FLARE' Activity types
        self.emissions_df[ch4_result] = numpy.where(
            self.emissions_df[fuel_type] == 'FUEL', self.emissions_df[volume_col] * 1000 * self.emissions_df[ch4_ef],
            numpy.where(self.emissions_df[fuel_type] == 'FLARE', 
                     self.emissions_df[volume_col] * 1000 * self.emissions_df[ch4_ef] * 0.000001, 0)
            )
        
        # Calculate N2O Emissions for 'FUEL' and 'FLARE' Activity types
        self.emissions_df[n2o_result] = numpy.where(
            self.emissions_df[fuel_type] == 'FUEL', self.emissions_df[volume_col] * 1000 * self.emissions_df[n2o_ef],
            numpy.where(self.emissions_df[fuel_type] == 'FLARE', 
                     self.emissions_df[volume_col] * 1000 * self.emissions_df[n2o_ef] * 0.000001, 0)
            )
        
        self.emissions_df[total_emission] = (self.emissions_df[co2_result] * self.co2_gwp) + (self.emissions_df[ch4_result] * self.ch4_gwp) +(self.emissions_df[n2o_result] * self.n2o_gwp)

        return self.emissions_df  
    
# Class used to perform production calculation
class ConOilnGasProductionCalc:
    def __init__(self, production_df):
        self.production_df = production_df  #Load the production DataFrame into the class
    
    #Calculate production in M3OE
    def calculate_production (self, volume, prod_conversion_factor, total_production) -> float:
        """
        This function calculates the total production for each facility volumetric record: 
        Equation:
            Production(m3OE) = Volume * Conversion_Factor 
        """
        # Calculate Production
        self.production_df[total_production] = self.production_df[volume] * self.production_df[prod_conversion_factor]

        return self.production_df
