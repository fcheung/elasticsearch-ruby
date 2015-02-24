require 'test_helper'

module Elasticsearch
  module Test
    class FiltersIntegrationTest < ::Elasticsearch::Test::IntegrationTestCase
      include Elasticsearch::DSL::Search

      context "Query integration" do
        startup do
          Elasticsearch::Extensions::Test::Cluster.start(nodes: 1) if ENV['SERVER'] and not Elasticsearch::Extensions::Test::Cluster.running?
        end

        setup do
          @client.indices.create index: 'test'
          @client.index index: 'test', type: 'd', body: { name: 'Original', color: 'red',  size: 'xxl' }
          @client.index index: 'test', type: 'd', body: { name: 'Original', color: 'red',  size: 'm' }
          @client.index index: 'test', type: 'd', body: { name: 'Modern',   color: 'grey', size: 'l' }
          @client.index index: 'test', type: 'd', body: { name: 'Modern',   color: 'grey', size: 's' }
          @client.indices.refresh index: 'test'
        end

        context "and/or/not filters" do
          should "find the document with and" do
            response = @client.search index: 'test', body: search {
              query do
                filtered do
                  filter do
                    _and do
                      term color: 'red'
                      term size:  'xxl'
                    end
                  end
                end
              end
            }.to_hash

            assert_equal 1, response['hits']['total']
          end

          should "find the documents with or" do
            response = @client.search index: 'test', body: search {
              query do
                filtered do
                  filter do
                    _or do
                      term size: 'l'
                      term size: 'm'
                    end
                  end
                end
              end
            }.to_hash

            assert_equal 2, response['hits']['total']
            assert response['hits']['hits'].all? { |h| ['l', 'm'].include? h['_source']['size']  }
          end

          should "find the documents with not as a Hash" do
            response = @client.search index: 'test', body: search {
              query do
                filtered do
                  filter do
                    _not term: { size: 'xxl' }
                  end
                end
              end
            }.to_hash

            assert_equal 3, response['hits']['total']
            assert response['hits']['hits'].none? { |h| h['_source']['size'] == 'xxl' }
          end

          should "find the documents with not as a block" do
            response = @client.search index: 'test', body: search {
              query do
                filtered do
                  filter do
                    _not do
                      term size: 'xxl'
                    end
                  end
                end
              end
            }.to_hash

            assert_equal 3, response['hits']['total']
            assert response['hits']['hits'].none? { |h| h['_source']['size'] == 'xxl' }
          end
        end
      end
    end
  end
end
