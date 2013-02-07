require './env'

seed_count = ENV.fetch('SEED_COUNT', 1_000_000).to_i

seed_count.times { OrderEvent.sample_order }
