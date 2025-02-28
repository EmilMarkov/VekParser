from vek_parser import VekParser
import logging

if __name__ == "__main__":
    logging.root.handlers = []
    
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('parser.log'),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info("Запуск программы")

    parser = VekParser(
        config_path='config.yml',
        base_url='https://example.com',
        headers={
            'User-Agent': 'custom user agent'
        }
    )

    parser.run(initial_context={
        'example_data': [
            {"url": "https://example.com/1"},
            {"url": "https://example.com/2"},
            {"url": "https://example.com/3"}
        ]
    })

    parser.save_data('collected_data.json')
    
    logger.info("Программа завершена")