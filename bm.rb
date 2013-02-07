require './env'
require 'benchmark'

def measure(kind)
  puts "#{kind}:"
  OrderEvent.send("#{kind}!")

  Benchmark.benchmark(Benchmark::CAPTION, 12, Benchmark::FORMAT, "total:") do |x|
    avg = x.report('avg purchase') { OrderEvent.average_purchase }
    top = x.report('top products') { OrderEvent.top_products }
    [avg + top]
  end

  puts
end

measure :naive
measure :db_eval
measure :map_reduce
measure :aggregation
