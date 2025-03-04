import re
import logging
from typing import Dict

from web_parser import WebParser


class AreaCalculator:
    @staticmethod
    def calculate_dimensions(step_config: Dict, context: Dict) -> Dict:
        result = {}
        try:
            measures = {
                'width': context.get('width', '0'),
                'height': context.get('height', '0'),
                'depth': context.get('depth', '0')
            }

            dimensions = {}
            for key, value in measures.items():
                cleaned = re.sub(r'[^\d.,]', '', str(value).replace(',', '.')).strip()
                dimensions[key] = float(cleaned) if cleaned else 0.0

            result.update({
                'width': f"{dimensions['width']} см",
                'height': f"{dimensions['height']} см",
                'depth': f"{dimensions['depth']} см",
                'area': round(dimensions['width'] * dimensions['height'], 2),
                'volume': round(dimensions['width'] * dimensions['height'] * dimensions['depth'], 2),
                'sku': context.get('sku')  # Пробрасываем sku напрямую
            })

            if 'price' in context:
                result['price'] = int(str(context['price']).replace('\xa0', '').replace(' ', ''))

        except Exception as e:
            logging.error(f"Dimension calculation error: {str(e)}")
            result.update({
                'width': None,
                'height': None,
                'depth': None,
                'area': None,
                'volume': None,
                'sku': None
            })

        return result

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = WebParser(
        config_path='config.yml',
        base_url='https://parsemachine.com',
        render_js=False
    )

    parser.register_step_handler('transform', AreaCalculator.calculate_dimensions)

    parser.register_item_handler('dict', lambda item, _: item)

    with parser:
        parser.run()
        parser.save_data('full_catalog_data.json')
