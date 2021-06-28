#
# Fluentd
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#

require 'fluent/plugin/filter'
require 'fluent/config/error'
require 'fluent/plugin/string_util'

def filter_regex(cond, record)
    reg = cond[:regexp].each { |k, p|
      return false if !p.match?(record[k])
    }

    exc = cond[:exclude].map { |k, p|
      return true if !p.match?(record[k])
    }
    return true
end

module Fluent::Plugin
  class GrepFilter < Filter
    Fluent::Plugin.register_filter('grep_ractor', self)

    def initialize
      super
    end

    helpers :record_accessor

    config_section :regexp, param_name: :regexps, multi: true do
      desc "The field name to which the regular expression is applied."
      config_param :key, :string
      desc "The regular expression."
      config_param :pattern, :regexp
    end

    config_section :exclude, param_name: :excludes, multi: true do
      desc "The field name to which the regular expression is applied."
      config_param :key, :string
      desc "The regular expression."
      config_param :pattern, :regexp
    end

    def configure(conf)
      super

      @cond = { regexp: {}, exclude: {}}

      @regexps.each do |e|
        @cond[:regexp][e.key] = e.pattern
      end

      @excludes.each do |e|
        @cond[:exclude][e.key] = e.pattern
      end

      @workers = 2.times.map {
        Ractor.new @cond do |cond|
          while true
            marked = Ractor.receive.map { |time, record|
              filter_regex(cond, record)
            }
            Ractor.yield marked.freeze
          end
        end
      }
    end

#    def filter_stream(tag, es)
#        new_es = Fluent::MultiEventStream.new
#        es.each do |time, record|
#          new_es.add(time, record) if filter_regex(@cond, record)
#        end
#        new_es
#    end

    def filter_stream(tag, es)
        half = es.size / 2

        @workers[0].send(es.slice(0, half))
        @workers[1].send(es.slice(half, es.size))

        new_es = Fluent::MultiEventStream.new
        marked = [].concat(@workers[0].take, @workers[1].take)

        i = 0
        es.each do |time, record|
          new_es.add(time, record) if marked[i]
          i += 1
        end
        new_es
    end
  end
end
