use v6;

# =begin Pod
#
# =head1 Redis
#
# C<Redis> is a Perl6 binding for Redis database.
#
# =head1 Synopsis
#
#   my $redis = Redis.new("192.168.1.12:6379");
#   $redis.connect();
#   $redis.set("key", "value");
#   say $redis.get("key");
#
# =end Pod

# Initiate callbacks
my &status_code_reply_cb = { $_ eq "OK" };
my &integer_reply_cb = { $_.Bool };
my &string_to_float_cb = { $_.Real };
my %command_callbacks = ();
%command_callbacks{"PING"} = { $_ eq "PONG" };
for "QUIT SET MSET PSETEX SETEX MIGRATE RENAME RENAMENX RESTORE HMSET".split(" ") -> $c {
    %command_callbacks{$c} = &status_code_reply_cb;
}
for "EXISTS SETNX EXPIRE EXPIREAT MOVE PERSIST PEXPIRE PEXPIREAT HSET HEXISTS HSETNX".split(" ") -> $c {
    %command_callbacks{$c} = &integer_reply_cb;
}
for "INCRBYFLOAT HINCRBYFLOAT".split(" ") -> $c {
    %command_callbacks{$c} = &string_to_float_cb;
}
%command_callbacks{"HGETALL"} = {
        # TODO so ugly...
        my %h = ();
        my $l = $_;
        for $_.pairs {
            if .key % 2 eq 0 {
                %h{.value} = $l[.key + 1];
            }
        }
        return %h;
    };

class Redis;

has Str $.host = '127.0.0.1';
has Int $.port = 6379;
has Str $.sock; # if sock is defined, use sock
has Bool $.debug = False;
has Real $.timeout = 0.0; # 0 means unlimited
has $.conn is rw;
has %!command_callbacks = %command_callbacks;

method new(Str $server?, Bool :$debug?, Real :$timeout?) {
    my %config = {}
    if $server.defined {
        if $server ~~ m/^([\d+]+ %\.) [':' (\d+)]?$/ {
            %config<host> = $0.Str;
            if $1 {
                %config<port> = $1.Str.Int;
            }
        } else {
            %config<sock> = $server;
        }
    }
    if $debug.defined {
        %config<debug> = $debug;
    }
    if $timeout.defined {
        %config<timeout> = $timeout;
    }
    return self.bless(*, |%config);
}

method connect {
    if $.sock.defined {
        die "Sorry, connecting via unix sock is currently unsupported!";
    } else {
        $.conn = IO::Socket::INET.new(host => $.host, port => $.port, input-line-separator => "\r\n");
    }
}

method !pack_command(*@args) {
    my $cmd = '*' ~ @args.elems ~ "\r\n";
    for @args -> $arg {
        $cmd ~= '$';
        $cmd ~= $arg.chars;
        $cmd ~= "\r\n";
        $cmd ~= $arg;
        $cmd ~= "\r\n";
    }
    return $cmd;
}


method !send_command(*@args) {
    $.conn.send(self!pack_command(|@args));
}

method !exec_command(*@args) {
    if @args.elems <= 0 {
        die "Invalid command.";
    }
    my Str $cmd = @args[0];
    self!send_command(|@args);
    return self!parse_response(self!read_response(), $cmd);
}

# call get on Socket::IO, don't use recv, cuz they are use different buffer.
method !read_response {
    my $first-line = $.conn.get();
    my ($flag, $response) = $first-line.substr(0, 1), $first-line.substr(1);
    if $flag !eq any('+', '-', ':', '$', '*') {
        die "Unknown response from redis!\n";
    }
    if $flag eq '+' {
        # single line reply, pass
    } elsif $flag eq '-' {
        # on error
        die $response;
    } elsif $flag eq ':' {
        # int value
        $response = $response.Int;
    } elsif $flag eq '$' {
        # bulk response
        my $length = $response.Int;
        if $length eq -1 {
            return Nil;
        }
        $response = $.conn.get();
        if $response.chars !eq $length {
            die "Invalid response.";
        }
    } elsif $flag eq '*' {
        # multi-bulk response
        my $length = $response.Int;
        if $length eq -1 {
            return Nil;
        }
        $response = [];
        for 1..$length {
            $response.push(self!read_response());
        }
    }
    return $response;
}

method !parse_response($response, $command) {
    if %!command_callbacks.exists($command) {
        return %!command_callbacks{$command}($response);
    }
    return $response;
}

# Ping the server.
method ping returns Bool {
    return self!exec_command("PING");
}

# Ask the server to close the connection. The connection is closed as soon as all pending replies have been written to the client.
method quit returns Bool {
    return self!exec_command("QUIT");
}

###### Commands/Keys #######

method del(*@keys) returns Int {
    return self!exec_command("DEL", |@keys);
}

method dump(Str $key) returns Str {
    return self!exec_command("DUMP", $key);
}

method exists(Str $key) returns Bool {
    return self!exec_command("EXISTS", $key);
}

method expire(Str $key, Int $seconds) returns Bool {
    return self!exec_command("EXPIRE", $key, $seconds);
}

method expireat(Str $key, Int $timestamp) returns Bool {
    return self!exec_command("EXPIREAT", $key, $timestamp);
}

method ttl(Str $key) returns Int {
    return self!exec_command("TTL", $key);
}

method keys(Str $pattern) returns Array {
    return self!exec_command("KEYS", $pattern);
}

method migrate(Str $host, Int $port, Str $key, Str $destination-db, Int $timeout) returns Bool {
    return self!exec_command("MIGRATE", $host, $port, $key, $destination-db, $timeout);
}

method move(Str $key, Str $db) returns Bool {
    return self!exec_command("MOVE", $key, $db);
}

method object(Str $subcommand, *@arguments) {
    return self!exec_command("OBJECT", $subcommand, |@arguments);
}

method persist(Str $key) returns Bool {
    return self!exec_command("PERSIST", $key);
}

method pexpire(Str $key, Int $milliseconds) returns Bool {
    return self!exec_command("PEXPIRE", $key, $milliseconds);
}

method pexpireat(Str $key, Int $milliseconds-timestamp) returns Bool {
    return self!exec_command("PEXPIREAT", $key, $milliseconds-timestamp);
}

method pttl(Str $key) returns Int {
    return self!exec_command("TTL", $key);
}

method randomkey() {
    return self!exec_command("RANDOMKEY");
}

method rename(Str $key, Str $newkey) returns Bool {
    return self!exec_command("RENAME", $key, $newkey);
}

method renamenx(Str $key, Str $newkey) returns Bool {
    return self!exec_command("RENAMENX", $key, $newkey);
}

method restore(Str $key, Int $milliseconds, Str $serialized-value) returns Bool {
    return self!exec_command("RESTORE", $key, $milliseconds, $serialized-value);
}

method sort(Str $key, Str :$by?,
        Int :$offset?, Int :$count?, 
        :@get?,
        Bool :$desc = False,
        Bool :$alpha = False,
        Str :$store?
    ) returns Array {
    if ($offset.defined and !$count.defined) or (!$offset.defined and $count.defined) {
        die "`offset` and `count` must both be specified.";
    }
    say $desc;
    # TODO
    return []
}

# Returns the string representation of the type of the value stored at key. The
# different types that can be returned are: none, string, list, set, zset and hash.
method type(Str $key) {
    return self!exec_command("TYPE", $key);
}

###### ! Commands/Keys #######

###### Commands/Strings ######

method append(Str $key, $value) returns Int {
    return self!exec_command("APPEND", $key, $value);
}

method bitcount(Str $key, Int $start?, Int $end?) returns Int {
    my @args = [$key];
    if $start.defined and $end.defined {
        @args.push($start);
        @args.push($end);
    } elsif $start.defined or $end.defined {
        die "Both start and end must be specified.";
    }
    return self!exec_command("BITCOUNT", |@args);
}

method bitop(Str $op, Str $key, *@keys) {
    return self!exec_command("BITOP", $op, $key, |@keys);
}

method get(Str $key) {
    return self!exec_command("GET", $key);
}

method set(Str $key, $value) returns Bool {
    return self!exec_command("SET", $key, $value);
}

method setbit(Str $key, Int $offset, $value) returns Int {
    return self!exec_command("SETBIT", $key, $offset, $value);
}

method setex(Str $key, Int $seconds, $value) {
    return self!exec_command("SETEX", $key, $seconds, $value);
}

method setnx(Str $key, $value) returns Bool {
    return self!exec_command("SETNX", $key, $value);
}

method setrange(Str $key, Int $offset, $value) returns Int {
    return self!exec_command("SETRANGE", $key, $offset, $value);
}

method strlen(Str $key) returns Int {
    return self!exec_command("STRLEN", $key);
}

method getbit(Str $key, Int $offset) returns Int {
    return self!exec_command("GETBIT", $key, $offset);
}

method getrange(Str $key, Int $start, Int $end) returns Str {
    return self!exec_command("GETRANGE", $key, $start, $end);
}

method getset(Str $key, $value) {
    return self!exec_command("GETSET", $key, $value);
}

method incrbyfloat(Str $key, Real $increment) returns Real {
    return self!exec_command("INCRBYFLOAT", $key, $increment);
}

method mget(*@keys) {
    return self!exec_command("MGET", |@keys);
}

# Sets the given keys to their respective values.
# Arguments can be named or positional parameters.
method mset(*@args, *%named) {
    for %named {
        @args.push(.key);
        @args.push(.value);
    }
    return self!exec_command("MSET", |@args);
}

method msetnx(*@args, *%named) {
    for %named {
        @args.push(.key);
        @args.push(.value);
    }
    return self!exec_command("MSETNX", |@args);
}

method psetex(Str $key, Int $milliseconds, $value) {
    return self!exec_command("PSETEX", $key, $milliseconds, $value);
}

method incr(Str $key) {
    return self!exec_command("INCR", $key);
}

method incrby(Str $key, Int $increment) {
    return self!exec_command("INCRBY", $key, $increment);
}

method decr(Str $key) {
    return self!exec_command("DECR", $key);
}

method decrby(Str $key, Int $increment) {
    return self!exec_command("DECRBY", $key, $increment);
}

###### ! Commands/Strings ######

###### Commands/Hashes ######
#
# field can be integer/string

method hdel(Str $key, *@fields) returns Int {
    return self!exec_command("HDEL", $key, |@fields);
}

method hexists(Str $key, $field) returns Bool {
    return self!exec_command("HEXISTS", $key, $field);
}

method hget(Str $key, $field) returns Any {
    return self!exec_command("HGET", $key, $field);
}

method hgetall(Str $key) returns Hash {
    return self!exec_command("HGETALL", $key);
}

method hincrby(Str $key, $field, Int $increment) returns Int {
    return self!exec_command("HINCRBY", $key, $field, $increment);
}

method hincrbyfloat(Str $key, $field, Real $increment) returns Real {
    return self!exec_command("HINCRBYFLOAT", $key, $field, $increment);
}

method hkeys(Str $key) returns Array {
    return self!exec_command("HKEYS", $key);
}

method hlen(Str $key) returns Int {
    return self!exec_command("HLEN", $key);
}

method hmget(Str $key, *@fields) returns Array {
    return self!exec_command("HMGET", $key, |@fields);
}

method hmset(Str $key, *@args, *%named) returns Bool {
    for %named {
        @args.push(.key);
        @args.push(.value);
    }
    return self!exec_command("HMSET", $key, |@args);
}

method hset(Str $key, $field, $value) returns Bool {
    return self!exec_command("HSET", $key, $field, $value);
}

method hsetnx(Str $key, $field, $value) returns Bool {
    return self!exec_command("HSETNX", $key, $field, $value);
}

method hvals(Str $key) returns Array {
    return self!exec_command("HVALS", $key);
}

###### ! Commands/Hashes ######

# vim: ft=perl6
