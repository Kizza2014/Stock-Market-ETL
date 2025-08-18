from extract import (MostActiveQuoteCrawler, HistoryCrawler, ProfileCrawler, StatisticsCrawler,
                     IncomeStatementCrawler)
from load import (MostActiveQuoteParser, HistoryParser, ProfileParser, StatisticsParser,
                  IncomeStatementParser, BalanceSheetParser, CashFlowParser)
from transform import (CompanyTransformer, BalanceSheetTransformer, CashFlowTransformer, HistoryTransformer,
                       StatisticsTransformer, IncomeStatementTransformer)
from datetime import date
from dotenv import load_dotenv


load_dotenv()  # Load environment variables from .env file


def crawl_active_tickers(crawl_date):
    print("\n\n================== MOST ACTIVE QUOTES CRAWLING ==================\n")
    crawler = MostActiveQuoteCrawler()
    crawler.crawl_all_tickers(crawl_date=crawl_date)
    print("\nCrawling completed.")

def parse_active_tickers(parse_date):
    print("\n\n================== MOST ACTIVE QUOTES PARSING ==================\n")
    parser = MostActiveQuoteParser()
    most_active_tickers = parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")
    return most_active_tickers

def crawl_histories(tickers, crawl_date):
    print("\n\n================== HISTORY CRAWLING ==================\n")
    crawler = HistoryCrawler()
    crawler.crawl_all_daily_histories(tickers=tickers, crawl_date=crawl_date)
    print("\nCrawling completed.")

def parse_histories(parse_date):
    print("\n\n================== HISTORY PARSING ==================\n")
    parser = HistoryParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def crawl_profiles(tickers, crawl_date):
    print("\n\n================== PROFILE CRAWLING ==================\n")
    crawler = ProfileCrawler()
    crawler.crawl_profile(tickers, crawl_date)
    print("\nCrawling completed.")

def parse_profiles(parse_date):
    print("\n\n================== PROFILE PARSING ==================\n")
    parser = ProfileParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def crawl_statistics(tickers, crawl_date):
    print("\n\n================== STATISTICS CRAWLING ==================\n")
    crawler = StatisticsCrawler()
    crawler.crawl_statistics(tickers, crawl_date=crawl_date)
    print("\nCrawling completed.") 

def parse_statistics(parse_date):
    print("\n\n================== STATISTICS PARSING ==================\n")
    parser = StatisticsParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def crawl_income_statement(tickers, crawl_date):
    print("\n\n================== INCOME STATEMENT CRAWLING ==================\n")
    crawler = IncomeStatementCrawler()
    crawler.crawl_income_statement(tickers, crawl_date=crawl_date)
    print("\nCrawling completed.")

def parse_income_statement(parse_date):
    print("\n\n================== INCOME STATEMENT PARSING ==================\n")
    parser = IncomeStatementParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def parse_balance_sheet(parse_date):
    print("\n\n================== BALANCE SHEET PARSING ==================\n")
    parser = BalanceSheetParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def parse_cash_flow(parse_date):
    print("\n\n================== CASH FLOW PARSING ==================\n")
    parser = CashFlowParser()
    parser.parse_all_html(parse_date=parse_date)
    print("\nParsing completed.")

def transform_profile(transform_date):
    print("\n\n================== PROFILE TRANSFORMATION ==================\n")
    transformer = CompanyTransformer()
    transformer.process(f"type=profile/date={transform_date}/")
    print("\nTransformation completed.")

def transform_history(transform_date):
    print("\n\n================== HISTORY TRANSFORMATION ==================\n")
    transformer = HistoryTransformer()
    transformer.process(f"type=history/date={transform_date}/")
    print("\nTransformation completed.")

def transform_income_statement(transform_date):
    print("\n\n================== INCOME STATEMENT TRANSFORMATION ==================\n")
    transformer = IncomeStatementTransformer()
    transformer.process(f"type=income_statement/date={transform_date}/")
    print("\nTransformation completed.")

def transform_balance_sheet(transform_date):
    print("\n\n================== BALANCE SHEET TRANSFORMATION ==================\n")
    transformer = BalanceSheetTransformer()
    transformer.process(f"type=balance_sheet/date={transform_date}/")
    print("\nTransformation completed.")

def transform_cash_flow(transform_date):
    print("\n\n================== CASH FLOW TRANSFORMATION ==================\n")
    transformer = CashFlowTransformer()
    transformer.process(f"type=cash_flow/date={transform_date}/")
    print("\nTransformation completed.")

def transform_statistics(transform_date):
    print("\n\n================== STATISTICS TRANSFORMATION ==================\n")
    transformer = StatisticsTransformer()
    transformer.process(f"type=statistics/date={transform_date}/")
    print("\nTransformation completed.")


if __name__ == "__main__":
    load_dotenv()  # Load environment variables from .env file

    # crawl_date = date.today().strftime("%Y_%m_%d")
    crawl_date = "2025_08_02"
    print(f"Crawling date: {crawl_date}")

    # # crawl active tickers
    # crawl_active_tickers(crawl_date=crawl_date)

    # # parse active tickers
    # most_active_tickers = parse_active_tickers(parse_date=crawl_date) # liệt kê danh sách tickers để crawl thông tin chi tiết

    # # crawl history
    # crawl_histories(tickers=most_active_tickers, crawl_date=crawl_date)

    # # parse history
    # parse_histories(parse_date=crawl_date)

    # # crawl profile
    # crawl_profiles(tickers=most_active_tickers, crawl_date=crawl_date)

    # # parse profile
    # parse_profiles(parse_date=crawl_date)

    # # crawl statistics
    # crawl_statistics(most_active_tickers, crawl_date=crawl_date)

    # # parse statistics
    # parse_statistics(parse_date=crawl_date)

    # #  crawl income statement
    # crawl_income_statement(tickers=most_active_tickers, crawl_date=crawl_date)

    # # parse income statment
    # parse_income_statement(parse_date=crawl_date)

    # # parse balance sheet
    # parse_balance_sheet(parse_date=crawl_date)

    # # parse cash flow
    # parse_cash_flow(parse_date=crawl_date)

    # transform profile
    transform_profile(transform_date=crawl_date)

    # transform history
    transform_history(transform_date=crawl_date)

    # transform income statement
    transform_income_statement(transform_date=crawl_date)

    # transform balance sheet
    transform_balance_sheet(transform_date=crawl_date)

    # transform cash flow
    transform_cash_flow(transform_date=crawl_date)

    # transform statistics
    transform_statistics(transform_date=crawl_date)