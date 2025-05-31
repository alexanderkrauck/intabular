import pandas as pd

def create_test_subset():
    # Load the original CSV file
    df = pd.read_csv('data/target_with_emails.csv')
    
    print(f"Original dataset shape: {df.shape}")
    print(f"First 10 rows preview:")
    print(df.head(10))
    
    # Take first 10 rows
    test_df = df.head(10)
    
    # Save to a new test file
    test_df.to_csv('data/target_with_emails_test10.csv', index=False)
    
    print(f"\nTest subset saved to 'data/target_with_emails_test10.csv' with shape: {test_df.shape}")

if __name__ == "__main__":
    create_test_subset() 