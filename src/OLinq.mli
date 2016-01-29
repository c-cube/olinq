
(* This file is free software, part of OLinq. See file "license" for more details. *)

(** {1 LINQ-like operations on collections}

    The purpose is to provide powerful combinators to express iteration,
    transformation and combination of collections of items.

    Functions and operations are assumed to be referentially transparent, i.e.
    they should not rely on external side effects, they should not rely on
    the order of execution.

    {[

      OLinq.(
        of_list [1;2;3]
        |> flat_map (fun x -> (x -- (x+10)))
        |> count ()
        |> flat_map of_pmap
        |> sort ()
        |> run_list
      );;

      - : (int * int) list = [(1, 1); (2, 2); (3, 3); (4, 3); (5, 3); (6, 3);
                              (7, 3); (8, 3); (9, 3); (10, 3); (11, 3); (12, 2); (13, 1)]

        OLinq.(
          IO.read_file "/tmp/foo"
          |> IO.lines
          |> sort ()
          |> IO.to_file_lines "/tmp/bar"
        );;
      - :  `Ok ()
    ]}

*)

type 'a sequence = ('a -> unit) -> unit
type 'a equal = 'a -> 'a -> bool
type 'a ord = 'a -> 'a -> int
type 'a hash = 'a -> int
type 'a or_error = [`Ok of 'a | `Error of string ]

(** {2 Polymorphic Maps} *)
module PMap : sig
  type ('a, +'b) t

  val get : ('a,'b) t -> 'a -> 'b option

  val size : (_,_) t -> int

  val to_seq : ('a, 'b) t -> ('a * 'b) sequence

  val to_list : ('a, 'b) t -> ('a * 'b) list

  val map : ('b -> 'c) -> ('a, 'b) t -> ('a, 'c) t
  (** Transform values *)

  val reverse : ?cmp:'b ord -> ?eq:'b equal -> ?hash:'b hash -> unit ->
    ('a,'b) t -> ('b,'a list) t
  (** Reverse relation of the map, as a multimap *)

  val reverse_multimap : ?cmp:'b ord -> ?eq:'b equal -> ?hash:'b hash -> unit ->
    ('a,'b list) t -> ('b,'a list) t
  (** Reverse relation of the multimap *)

  val fold : ('acc -> 'a -> 'b -> 'acc) -> 'acc -> ('a,'b) t -> 'acc
  (** Fold on the items of the map *)

  val fold_multimap : ('acc -> 'a -> 'b -> 'acc) -> 'acc ->
    ('a,'b list) t -> 'acc
  (** Fold on the items of the multimap *)

  val get_seq : 'a -> ('a, 'b) t -> 'b sequence
  (** Select a key from a map and wrap into sequence *)

  val iter : ('a,'b) t -> ('a*'b) sequence
  (** View a multimap as a proper collection *)

  val flatten : ('a,'b sequence) t -> ('a*'b) sequence
  (** View a multimap as a collection of individual key/value pairs *)

  val flatten_l : ('a,'b list) t -> ('a*'b) sequence
  (** View a multimap as a list of individual key/value pairs *)
end

(** {2 Main Type} *)

type ('a, 'card) t constraint 'card = [<`One | `AtMostOne | `Any]
(** Type of a query that returns zero, one or more values of type 'a.
    The parameter ['card] indicates how many elements are in the collection,
    with [`Any] indicating the number is unknown, [`AtMostOne] that there
    are 0 or 1 elements and [`One] exactly one.

    Conceptually, the cardinalities are ordered from most precise (`One)
    to least precise (`Any):  `One < `AtMostOne < `Any. *)

type 'a t_any = ('a, [`Any]) t
type 'a t_one = ('a, [`One]) t
type 'a t_at_most_one = ('a, [`AtMostOne]) t

(** {2 Initial values} *)

val empty : ('a, [`AtMostOne]) t
(** Empty collection *)

val return : 'a -> ('a, [>`One]) t
(** Return one value *)

val of_list : 'a list -> ('a, [`Any]) t
(** Query that just returns the elements of the list *)

val of_array : 'a array -> ('a, [`Any]) t
val of_array_i : 'a array -> (int * 'a, [`Any]) t

val range : int -> int -> (int, [`Any]) t
(** [range i j] goes from [i] up to [j] included *)

val (--) : int -> int -> (int, [`Any]) t
(** Synonym to {!range} *)

val of_hashtbl : ('a,'b) Hashtbl.t -> ('a * 'b, [`Any]) t

val of_seq : 'a sequence -> ('a, [`Any]) t
(** Query that returns the elements of the given sequence. *)

val of_queue : 'a Queue.t -> ('a, [`Any]) t

val of_stack : 'a Stack.t -> ('a, [`Any]) t

val of_string : string -> (char, [`Any]) t
(** Traverse the characters of the string *)

val of_pmap : ('a, 'b) PMap.t -> ('a * 'b, [`Any]) t
(** [of_pmap m] yields each binding of [m] *)

(** {6 Execution} *)

val run : ?limit:int -> ('a, _) t -> 'a sequence
(** Execute the query, possibly returning an error if things go wrong
    @param limit max number of values to return *)

val run_list : ?limit:int -> ('a, _) t -> 'a list

val run_array : ?limit:int -> ('a, _) t -> 'a array

val run1 : ('a, [`One]) t -> 'a
(** Run the query and return the only value *)

val run1_exn : ('a, _) t -> 'a
(** @raise Not_found if the query contains 0 element *)

(** {6 Basics} *)

val map : ('a -> 'b) -> ('a, 'card) t -> ('b, 'card) t
(** Map each value *)

val (>|=) : ('a, 'card) t -> ('a -> 'b) -> ('b, 'card) t
(** Infix synonym of {!map} *)

val filter : ('a -> bool) -> ('a, _) t -> ('a, [`Any]) t
(** Filter out values that do not satisfy predicate. We lose precision
    on the cardinality because of type system constraints. *)

val size : _ t -> (int, [>`One]) t
(** [size t] returns one value, the number of items returned by [t] *)

val choose : ('a, _) t -> ('a, [>`AtMostOne]) t
(** Choose one element (if any, otherwise empty) in the collection.
    This is like a "cut" in prolog. *)

val filter_map : ('a -> 'b option) -> ('a, _) t -> ('b, [`Any]) t
(** Filter and map elements at once *)

val flat_map_seq : ('a -> 'b sequence) -> ('a, _) t -> ('b, [`Any]) t
(** Same as {!flat_map} but using sequences *)

val flat_map_l : ('a -> 'b list) -> ('a, _) t -> ('b, [`Any]) t
(** map each element to a collection and flatten the result *)

val flatten : ('a list, _) t -> ('a, [`Any]) t

val flatten_seq : ('a sequence,_) t -> ('a, [`Any]) t

val take : int -> ('a, _) t -> ('a, [`Any]) t
(** Take at most [n] elements *)

val take1 : ('a, _) t -> ('a, [>`AtMostOne]) t
(** Specialized version of {!take} that keeps only the first element *)

val take_while : ('a -> bool) -> ('a, _) t -> ('a, [`Any]) t
(** Take elements while they satisfy a predicate *)

val sort : ?cmp:'a ord -> unit -> ('a, [`Any]) t -> ('a, [`Any]) t
(** Sort items by the given comparison function. Only meaningful when
    there are potentially many elements *)

val sort_by : ?cmp:'b ord -> ('a -> 'b) -> ('a, [`Any]) t -> ('a, [`Any]) t
(** [sort_by proj c] sorts the collection [c] by projecting elements using
    [proj], then using [cmp] to order them *)

val distinct : ?cmp:'a ord -> unit -> ('a, [`Any]) t -> ('a, [`Any]) t
(** Remove duplicate elements from the input collection.
    All elements in the result are distinct. *)

(** {6 Aggregation} *)

val group_by : ?cmp:'b ord -> ?eq:'b equal -> ?hash:'b hash ->
  ('a -> 'b) -> ('a, [`Any]) t -> (('b,'a list) PMap.t, [>`One]) t
(** [group_by f] takes a collection [c] as input, and returns
    a multimap [m] such that for each [x] in [c],
    [x] occurs in [m] under the key [f x]. In other words, [f] is used
    to obtain a key from [x], and [x] is added to the multimap using this key. *)

val group_by' : ?cmp:'b ord -> ?eq:'b equal -> ?hash:'b hash ->
  ('a -> 'b) -> ('a, [`Any]) t -> ('b * 'a list, [`Any]) t

val count : ?cmp:'a ord -> ?eq:'a equal -> ?hash:'a hash ->
  unit -> ('a, [`Any]) t -> (('a, int) PMap.t, [>`One]) t
(** [count c] returns a map from elements of [c] to the number
    of time those elements occur. *)

val count' : ?cmp:'a ord -> unit -> ('a, [`Any]) t -> ('a * int, [`Any]) t

val fold : ('b -> 'a -> 'b) -> 'b -> ('a, _) t -> ('b, [>`One]) t
(** Fold over the collection *)

val reduce :
  ('a -> 'b) -> ('a -> 'b -> 'b) -> ('b -> 'c) ->
  ('a,_) t -> ('c, [>`One]) t
(** [reduce start mix stop q] uses [start] on the first element of [q],
    and combine the result with following elements using [mix]. The final
    value is transformed using [stop]. *)

val is_empty : ('a, [<`AtMostOne | `Any]) t -> (bool, [>`One]) t

val sum : (int, [<`AtMostOne | `Any]) t -> (int, [>`One]) t

val contains : ?eq:'a equal -> 'a -> ('a, _) t -> (bool, [>`One]) t

val average : (int, _) t -> (int, [>`One]) t
val max : (int, _) t -> (int, [>`One]) t
val min : (int, _) t -> (int, [>`One]) t

val for_all : ('a -> bool) -> ('a, _) t -> (bool, [>`One]) t
val exists : ('a -> bool) -> ('a, _) t -> (bool, [>`One]) t
val find : ('a -> bool) -> ('a, _) t -> ('a option, [>`One]) t
val find_map : ('a -> 'b option) -> ('a, _) t -> ('b option, [>`One]) t

(** {6 Binary Operators} *)

val join : ?cmp:'key ord -> ?eq:'key equal -> ?hash:'key hash ->
  ('a -> 'key) -> ('b -> 'key) ->
  merge:('key -> 'a -> 'b -> 'c option) ->
  ('a, _) t -> ('b, _) t -> ('c, [`Any]) t
(** [join key1 key2 ~merge] is a binary operation
    that takes two collections [a] and [b], projects their
    elements resp. with [key1] and [key2], and combine
    values [(x,y)] from [(a,b)] with the same [key]
    using [merge]. If [merge] returns [None], the combination
    of values is discarded. *)

val group_join : ?cmp:'a ord -> ?eq:'a equal -> ?hash:'a hash ->
  ('b -> 'a) -> ('a,_) t -> ('b,_) t ->
  (('a, 'b list) PMap.t, [>`One]) t
(** [group_join key2] associates to every element [x] of
    the first collection, all the elements [y] of the second
    collection such that [eq x (key y)] *)

val product : ('a, _) t -> ('b,_) t -> ('a * 'b, [`Any]) t
(** Cartesian product *)

val append : ('a,_) t -> ('a,_) t -> ('a, [`Any]) t
(** Append two collections together *)

val inter : ?cmp:'a ord -> ?eq:'a equal -> ?hash:'a hash -> unit ->
  ('a,_) t -> ('a,_) t -> ('a,[`Any]) t
(** Intersection of two collections. Each element will occur at most once
    in the result *)

val union : ?cmp:'a ord -> ?eq:'a equal -> ?hash:'a hash -> unit ->
  ('a,_) t -> ('a,_) t -> ('a,[`Any]) t
(** Union of two collections. Each element will occur at most once
    in the result *)

val diff : ?cmp:'a ord -> ?eq:'a equal -> ?hash:'a hash -> unit ->
  ('a,_) t -> ('a,_) t -> ('a,[`Any]) t
(** Set difference *)

(** {6 Tuple and Options} *)

(** Specialized projection operators *)

val fst : ('a * 'b, 'card) t -> ('a, 'card) t

val snd : ('a * 'b, 'card) t -> ('b, 'card) t

val map_fst : ('a -> 'b) -> ('a * 'c, 'card) t -> ('b * 'c, 'card) t

val map_snd : ('a -> 'b) -> ('c * 'a, 'card) t -> ('c * 'b, 'card) t

val flatten_opt : ('a option, _) t -> ('a, [`Any]) t
(** Flatten the collection by removing [None] and mapping [Some x] to [x]. *)

(** {6 Applicative} *)

val pure : 'a -> ('a, _) t
(** Synonym to {!return} *)

val app : ('a -> 'b, 'card) t -> ('a, 'card) t -> ('b, 'card) t
(** Apply each function to each value. The cardinality should be the lowest
    upper bound of both input cardinalities (any,_) -> any, (one,one) -> one, etc. *)

val (<*>) : ('a -> 'b, 'card) t -> ('a, 'card) t -> ('b, 'card) t
(** Infix synonym to {!app} *)

(** {6 Monad}

    Careful, those operators do not allow any optimization before running the
    query, they might therefore be pretty slow. *)

val flat_map : ('a -> ('b, _) t) -> ('a,_) t -> ('b, [`Any]) t
(** Use the result of a query to build another query and immediately run it. *)

val (>>=) : ('a, _) t -> ('a -> ('b, _) t) -> ('b, [`Any]) t
(** Infix version of {!bind} *)

(** {6 Misc} *)

val lazy_ : ('a lazy_t, 'card) t -> ('a, 'card) t

exception UnwrapNone

val opt_unwrap_exn : ('a option, 'card) t -> ('a, 'card) t
(** @raise UnwrapNone if some option is None *)

val reflect_seq : ('a, _) t -> ('a sequence, [>`One]) t
(** [reflect q] evaluates all values in [q] and returns a sequence
    of all those values. Also blocks optimizations *)

val reflect_l : ('a, _) t -> ('a list, [>`One]) t
(** [reflect q] evaluates all values in [q] and returns a list
    of all those values. Also blocks optimizations *)

(* TODO: maybe a small vec type for efficient reflection *)

(** {6 Infix} *)

module Infix : sig
  val (--) : int -> int -> (int, [`Any]) t
  val (>|=) : ('a, 'card) t -> ('a -> 'b) -> ('b, 'card) t
  val (<*>) : ('a -> 'b, 'card) t -> ('a, 'card) t -> ('b, 'card) t
  val (>>=) : ('a, _) t -> ('a -> ('b, _) t) -> ('b, [`Any]) t
end

(** {6 Adapters} *)

val to_seq : ('a,_) t  -> ('a sequence, [>`One]) t
(** Build a (re-usable) sequence of elements, which can then be
    converted into other structures. Synonym to {!reflect_seq}.  *)

val to_hashtbl : (('a * 'b), _) t -> (('a, 'b) Hashtbl.t, [>`One]) t
(** Build a hashtable from the collection *)

val to_queue : ('a,_) t -> ('a Queue.t, [>`One]) t

val to_stack : ('a,_) t -> ('a Stack.t, [>`One]) t

module AdaptSet(S : Set.S) : sig
  val of_set : S.t -> (S.elt, [`Any]) t
  val reflect : (S.elt,_) t -> (S.t, [>`One]) t
  val run : (S.elt, _) t -> S.t
end

module AdaptMap(M : Map.S) : sig
  val of_map : 'a M.t -> (M.key * 'a, [`Any]) t
  val to_pmap : 'a M.t -> (M.key, 'a) PMap.t
  val reflect : (M.key * 'a, _) t -> ('a M.t, [`One]) t
  val run : (M.key * 'a, _) t -> 'a M.t
end

module IO : sig
  val read_chan : in_channel -> (string, [>`One]) t
  (** Read the content of the whole channel in (blocking), returning the
      corresponding string. The channel will be read at most once
      during execution, and its content cached; however the channel
      might never get read because evaluation is lazy. *)

  val read_file : string -> (string, [>`One]) t
  (** Read a whole file (given by name) and return its content as a string *)

  val lines : (string, _) t -> (string, [`Any]) t
  (** Convert a string into a collection of lines *)

  val lines_l : (string, 'card) t -> (string list, 'card) t
  (** Convert each string into a list of lines *)

  val join : string -> (string,_) t -> (string, [>`One]) t
  (** [join sep q] joins all the strings in [q] together,
      similar to [String.join sep (run_list q)] basically. *)

  val unlines : (string, _) t -> (string, [>`One]) t
  (** Join lines together *)

  val out : out_channel -> (string, _) t -> unit
  val out_lines : out_channel -> (string, _) t -> unit
  (** Evaluate the query and print it line by line on the output *)

  (** {8 Run methods} *)

  val to_file : string -> (string, _) t -> unit or_error
  val to_file_exn : string -> (string, _) t -> unit

  val to_file_lines : string -> (string, _) t -> unit or_error
  val to_file_lines_exn : string -> (string, _) t -> unit
end

(* TODO printer, table printer, ... ? *)

