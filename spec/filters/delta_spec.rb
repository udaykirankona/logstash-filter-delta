require 'spec_helper'
require "logstash/filters/delta"

describe LogStash::Filters::Delta do
  describe "Set to Hello World" do
    let(:config) do <<-CONFIG
      filter {
        delta {
          message => "Hello World"
        }
      }
    CONFIG
    end

    sample("message" => "some text") do
      expect(subject).to include("message")
      expect(subject['message']).to eq('Hello World')
    end
  end
end
