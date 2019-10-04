module Searchkick
  class ProcessBatchJob < ActiveJob::Base
    queue_as { Searchkick.queue_name }

    def perform(class_name:, record_ids:)
      klass = class_name.constantize
      scope = Searchkick.load_records(klass, record_ids)
      scope = scope.search_import if scope.respond_to?(:search_import)
      records = scope.select(&:should_index?)

      # determine which records to delete
      delete_ids = record_ids - records.map { |r| r.id.to_s }
      delete_records = delete_ids.map { |id| m = klass.new; m.id = id; m }

      # bulk reindex
      index = klass.searchkick_index
      with_rejection_handling(index) do
        Searchkick.callbacks(:bulk) do
          index.bulk_index(records) if records.any?
          index.bulk_delete(delete_records) if delete_records.any?

          if flipper_feature_enabled?(:flipper_fms_enable_shipping_es7)
            Er::Client.bulk_index(class_name, records)
            ER::Client.bulk_delete(class_name, delete_records)
          end
        end
      end
    end

    private

    def with_rejection_handling(index)
      begin
        yield
      rescue Searchkick::ImportError => e
        raise e unless e.failures

        retryable, non_retryable = e.failures.partition {|failed_item| failed_item['error']['type'] == 'es_rejected_execution_exception'}

        retryable.each do |failure|
          index.reindex_queue.push failure['_id']
        end

        raise e if non_retryable.any?
      end
    end
  end
end
