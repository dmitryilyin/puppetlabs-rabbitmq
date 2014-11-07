require 'set'

class Puppet::Provider::Rabbitmq_common < Puppet::Provider

  # Status functions
  ##################

  # get the RabbitMQ version number
  # @return [Float]
  def self.version
    return @version if @version
    status_text = rabbitmqctl '-q', 'status'
    status_text.split("\n").each do |line|
      begin
        if line =~ %r{"RabbitMQ","([\d\.]+)"}
          version = $1.split('.')
          @version =  "#{version[0]}.#{version[1..-1].join ''}".to_f
          return @version
        end
      rescue
        return @version = nil
      end
    end
    @version = nil
  end

  # get the RabbitMQ version number
  # resets mnemoisation
  # @return [Float]
  def self.version_with_renew
    @version = nil
    self.version
  end

  # check if this version supports tags
  # @return [TrueClass,FalseClass]
  def self.tag_support?
    self.version && self.version > 2.41
  end

  # User functions
  ################

  ADMIN_TAG = 'administrator'

  def self.parse_user_list_line_old(line)
    if line =~ %r{^(\S+)\s+(true|false)}
      user = $1.strip
      user_structure = {
          :tags => Set.new,
          :admin => $2 == 'true',
      }
      @user_list.store user, user_structure
    end
    @user_list
  end

  def self.parse_user_list_line_new(line)
    if line =~ %r{(.*?)\[(.*?)\]}
      user = $1.strip
      admin = false
      tags = []
      $2.split(',').each do |tag|
        tag.strip!
        if tag == ADMIN_TAG
          admin = true
        else
          tags << tag
        end
      end
      user_structure = {
          :tags => Set.new tags,
          :admin => admin,
      }
      @user_list.store user, user_structure
    end
    @user_list
  end

  # get the list of users and their tags
  # @return [Hash<String => Set>]
  def self.user_list
    return @user_list unless @user_list
    user_list_text = rabbitmqctl '-q', 'list_users'
    @user_list = {}
    user_list_text.split("\n").each do |line|
      if self.tag_support?
        self.parse_user_list_line_new line
      else
        self.parse_user_list_line_old line
      end
    end
    user_list
  end

  # get the list of users and their tags
  # resets mnemoisation
  # @return [Hash<String => Set>]
  def self.user_list_with_renew
    @user_list = nil
    self.user_list
  end

  # Wait functions
  ################

  # Wait 'count*step' seconds while RabbitMQ is ready (able to list its users&channels)
  # Make 'count' retries with 'step' delay between retries.
  # Limit each query time by 'timeout'
  def self.wait_for_online(count=30, step=6, timeout=10)
    count.times do |n|
      begin
        # Note, that then RabbitMQ cluster is broken or not ready, it might not show its
        # channels some times and hangs for ever, so we have to specify a timeout as well
        Timeout::timeout(timeout) do
          rabbitmqctl 'list_users'
          rabbitmqctl 'list_channels'
        end
      rescue Puppet::ExecutionFailure, Timeout
        Puppet.debug 'RabbitMQ is not ready, retrying'
        sleep step
      else
        Puppet.debug "RabbitMQ is online after #{n * step} seconds"
        return true
      end
    end
    raise Puppet::Error, "RabbitMQ is not ready after #{count * step} seconds expired!"
  end

  # retry the given code block until command suceeeds
  # for example:
  # users = self.class.run_with_retries { rabbitmqctl 'list_users' }
  def self.run_with_retries(count=30, step=6, timeout=10)
    count.times do |n|
      begin
        output = Timeout::timeout(timeout) do
          yield
        end
      rescue Puppet::ExecutionFailure, Timeout
        Puppet.debug 'Command failed, retrying'
        sleep step
      else
        Puppet.debug 'Command succeeded'
        return output
      end
    end
    raise Puppet::Error, "Command is still failing after #{count * step} seconds expired!"
  end

end