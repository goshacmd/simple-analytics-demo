class OrderLineItem
  include Mongoid::Document

  field :sku, type: String
  field :price, type: Float
  embedded_in :order_event
end

class OrderEvent
  module Naive
    def average_purchase
      all.map(&:total).sum / all.count
    end

    def top_products
      all.map(&:line_items).flatten.group_by(&:sku).map { |i| [i.first, i.last.count] }.sort_by(&:last).reverse
    end
  end

  module DbEval
    def average_purchase
      result = collection.database.command("$eval" => <<-EOJS)
        var orders = db.order_events.find(),
            order_count = orders.count(),
            total_sum = 0;

        orders.forEach(function(order) {
          total_sum += order.total;
        });

        return total_sum / order_count;
      EOJS

      result['retval']
    end

    def top_products
      result = collection.database.command("$eval" => <<-EOJS)
        var orders = db.order_events.find(),
            products = {};

        orders.forEach(function(order) {
          order.line_items.forEach(function(item) {
            var sku = item.sku;
            if (!products[sku]) products[sku] = 0;
            products[sku] += 1;
          });
        });

        return products;
      EOJS

      result['retval'].to_a.sort_by(&:last).reverse
    end
  end

  module MapReduce
    def average_purchase
      map = "function() { emit('avg', this); }"

      reduce = %Q{
        function(key, values) {
          var result = { sum: 0, count: 0 };
          values.forEach(function(value) {
            result.sum += value.total;
            result.count += 1;
          });
          return result;
        }
      }

      finalize = %Q{
        function(key, value) {
          value.avg = value.sum / value.count;
          return value;
        }
      }

      map_reduce(map, reduce).out(inline: 1).finalize(finalize).to_a.first['value']['avg']
    end

    def top_products
      map = %Q{
        function() {
          this.line_items.forEach(function(item) {
            emit(item.sku, item);
          });
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { purchases: 0 };
          values.forEach(function(value) {
            result.purchases += 1;
          });
          return result;
        }
      }

      items = map_reduce(map, reduce).out(inline: 1).to_a
      items.map { |doc| [doc['_id'], doc['value']['purchases']] }.sort_by(&:last).reverse
    end
  end

  module Aggregation
    def average_purchase
      collection.aggregate([
        {"$group" => {"_id" => "avg", "avg" => {"$avg" => "$total"}}}
      ]).first['avg']
    end

    def top_products
      collection.aggregate([
        {"$unwind" => "$line_items"},
        {"$project" => {"item" => "$line_items"}},
        {"$group" => {"_id" => "$item.sku", "purchases" => {"$sum" => 1}}},
        {"$sort" => {"purchases" => -1}}
      ]).map { |doc| [doc['_id'], doc['purchases']] }
    end
  end

  include Mongoid::Document

  field :total, type: Float
  embeds_many :line_items, class_name: 'OrderLineItem'

  class << self
    def naive!
      extend Naive
    end

    def db_eval!
      extend DbEval
    end

    def map_reduce!
      extend MapReduce
    end

    def aggregation!
      extend Aggregation
    end

    def sample_order(items = nil)
      line_items = []
      total = 0
      items = (1..10).to_a.sample unless items
      items.times do
        price = generate_price
        total += price
        line_items << { sku: generate_sku, price: price }
      end
      create(total: total, line_items: line_items)
    end

    def generate_sku
      (('A'..'Z').to_a * 3).shuffle.first(2).join
    end

    def generate_price
      (99..4999).to_a.sample / 100.0
    end
  end
end
