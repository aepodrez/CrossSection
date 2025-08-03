"""
Python equivalent of Frontier.do
Generated from: Frontier.do

Original Stata file: Frontier.do
"""

import pandas as pd
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
from sklearn.linear_model import LinearRegression

logger = logging.getLogger(__name__)

def frontier():
    """
    Python equivalent of Frontier.do
    
    Constructs the Frontier predictor signal for efficient frontier index.
    """
    logger.info("Constructing predictor signal: Frontier...")
    
    try:
        # DATA LOAD
        # Load SignalMasterTable data
        master_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/SignalMasterTable.csv")
        
        logger.info(f"Loading SignalMasterTable from: {master_path}")
        
        if not master_path.exists():
            logger.error(f"SignalMasterTable not found: {master_path}")
            logger.error("Please run the SignalMasterTable creation script first")
            return False
        
        # Load the required variables
        required_vars = ['permno', 'time_avail_m', 'mve_c', 'sicCRSP']
        
        data = pd.read_csv(master_path, usecols=required_vars)
        logger.info(f"Successfully loaded {len(data)} records")
        
        # Merge with Compustat annual data
        compustat_path = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Intermediate/m_aCompustat.csv")
        
        logger.info(f"Loading Compustat annual data from: {compustat_path}")
        
        if not compustat_path.exists():
            logger.error(f"Compustat annual file not found: {compustat_path}")
            logger.error("Please run the Compustat data download scripts first")
            return False
        
        # Load required variables from Compustat
        compustat_vars = ['permno', 'time_avail_m', 'at', 'ceq', 'dltt', 'capx', 'sale', 'xrd', 'xad', 'ppent', 'ebitda']
        compustat_data = pd.read_csv(compustat_path, usecols=compustat_vars)
        
        # Merge data (equivalent to Stata's "merge 1:1 permno time_avail_m using "$pathDataIntermediate/m_aCompustat", nogenerate keepusing(at ceq dltt at capx sale xrd xad ppent ebitda) keep(master match)")
        data = data.merge(
            compustat_data,
            on=['permno', 'time_avail_m'],
            how='inner'  # keep(master match)
        )
        
        logger.info(f"After merging with Compustat: {len(data)} observations")
        
        # SIGNAL CONSTRUCTION
        logger.info("Calculating Frontier signal...")
        
        # Replace missing xad with 0 (equivalent to Stata's "replace xad = 0 if mi(xad)")
        data['xad'] = data['xad'].fillna(0)
        
        # Create variables (equivalent to Stata's gen statements)
        data['YtempBM'] = np.log(data['mve_c'])
        data['tempBook'] = np.log(data['ceq'])
        data['tempLTDebt'] = data['dltt'] / data['at']
        data['tempCapx'] = data['capx'] / data['sale']
        data['tempRD'] = data['xrd'] / data['sale']
        data['tempAdv'] = data['xad'] / data['sale']
        data['tempPPE'] = data['ppent'] / data['at']
        data['tempEBIT'] = data['ebitda'] / data['at']
        
        # Create Fama-French 48 industry codes (simplified - would need actual mapping)
        # For now, use first 2 digits of SIC as approximation
        data['tempFF48'] = data['sicCRSP'].astype(str).str[:2].astype(int)
        
        # Drop missing tempFF48 (equivalent to Stata's "drop if mi(tempFF48)")
        data = data.dropna(subset=['tempFF48'])
        logger.info(f"After dropping missing industry codes: {len(data)} observations")
        
        # Initialize logmefit_NS
        data['logmefit_NS'] = np.nan
        
        # Get unique time periods
        time_periods = sorted(data['time_avail_m'].unique())
        
        logger.info(f"Running rolling regressions for {len(time_periods)} time periods...")
        
        # Run rolling regressions for each time period
        for t in time_periods:
            # Create training data (60 months before current period)
            train_data = data[
                (data['time_avail_m'] <= t) & 
                (data['time_avail_m'] > t - pd.DateOffset(months=60))
            ].copy()
            
            if len(train_data) > 100:  # Need sufficient observations
                # Prepare features for regression
                feature_cols = ['tempBook', 'tempLTDebt', 'tempCapx', 'tempRD', 'tempAdv', 'tempPPE', 'tempEBIT']
                
                # Add industry dummies
                for industry in train_data['tempFF48'].unique():
                    train_data[f'ind_{industry}'] = (train_data['tempFF48'] == industry).astype(int)
                
                # Get all industry dummy columns
                ind_cols = [col for col in train_data.columns if col.startswith('ind_')]
                
                # Prepare X and y for regression
                X = train_data[feature_cols + ind_cols].fillna(0)
                y = train_data['YtempBM'].fillna(0)
                
                try:
                    # Fit regression
                    model = LinearRegression()
                    model.fit(X, y)
                    
                    # Predict for current period
                    current_data = data[data['time_avail_m'] == t].copy()
                    
                    if len(current_data) > 0:
                        # Prepare features for prediction
                        for industry in current_data['tempFF48'].unique():
                            current_data[f'ind_{industry}'] = (current_data['tempFF48'] == industry).astype(int)
                        
                        X_pred = current_data[feature_cols + ind_cols].fillna(0)
                        
                        # Predict
                        predictions = model.predict(X_pred)
                        
                        # Update logmefit_NS
                        data.loc[data['time_avail_m'] == t, 'logmefit_NS'] = predictions
                        
                except Exception as e:
                    logger.warning(f"Regression failed for period {t}: {e}")
                    continue
        
        # Calculate Frontier (equivalent to Stata's "gen Frontier = YtempBM - logmefit_NS")
        data['Frontier'] = data['YtempBM'] - data['logmefit_NS']
        
        # Multiply by -1 (equivalent to Stata's "replace Frontier = -1*Frontier")
        data['Frontier'] = -1 * data['Frontier']
        
        logger.info("Successfully calculated Frontier signal")
        
        # Apply filters (equivalent to Stata's "drop if ceq == . | ceq < 0")
        data = data.dropna(subset=['ceq'])
        data = data[data['ceq'] >= 0]
        
        logger.info(f"After applying filters: {len(data)} observations")
        
        # SAVE RESULTS
        logger.info("Saving Frontier predictor signal...")
        
        # Create output directories if they don't exist
        predictors_dir = Path("/Users/alexpodrez/Documents/CrossSection/Signals/Data/Predictors")
        predictors_dir.mkdir(parents=True, exist_ok=True)
        
        # Prepare final dataset for saving
        output_data = data[['permno', 'time_avail_m', 'Frontier']].copy()
        
        # Remove missing values
        output_data = output_data.dropna(subset=['Frontier'])
        logger.info(f"Final dataset: {len(output_data)} observations")
        
        # Create yyyymm column for CSV output
        output_data['yyyymm'] = output_data['time_avail_m'].dt.year * 100 + output_data['time_avail_m'].dt.month
        
        # Save CSV file
        csv_output_path = predictors_dir / "Frontier.csv"
        csv_data = output_data[['permno', 'yyyymm', 'Frontier']].copy()
        csv_data.to_csv(csv_output_path, index=False)
        logger.info(f"Saved Frontier predictor to: {csv_output_path}")
        
        logger.info("Successfully constructed Frontier predictor signal")
        return True
        
    except Exception as e:
        logger.error(f"Failed to construct Frontier predictor: {e}")
        return False

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the predictor construction function
    frontier()
