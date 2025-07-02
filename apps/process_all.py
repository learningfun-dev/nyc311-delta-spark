'''
    Process all layers
'''

import process_bronze_layer
import process_silver_layer
import process_gold_layer
import process_embedding

def main():
    '''
        the main entry point for the application
    '''
    process_bronze_layer.main()
    process_silver_layer.main()
    process_gold_layer.main()
    process_embedding.main()

if __name__ == "__main__":
    main()
