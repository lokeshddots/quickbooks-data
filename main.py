from app.generators.main import QuickBooksGenerator

if __name__ == "__main__":
    # Initialize generator
    generator: QuickBooksGenerator = QuickBooksGenerator(business_id=1, number_of_years=1)

    # Run generator
    generator.run()
