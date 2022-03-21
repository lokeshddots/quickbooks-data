from app.generators.main import QuickBooksGenerator
import pdb

if __name__ == "__main__":
    print('Connection established')
    # Initialize generator
    # pdb.set_trace()
    generator: QuickBooksGenerator = QuickBooksGenerator(business_id=1, number_of_years=1)

    # Run generator
    generator.run()
