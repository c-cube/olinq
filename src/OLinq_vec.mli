
(* This file is free software, part of OLinq. See file "license" for more details. *)

(** {1 Resizable array} *)

type 'a sequence = ('a -> unit) -> unit

type 'a t = private {
  mutable size: int;
  mutable vec: 'a array;
}

val is_empty : _ t -> bool
val length : _ t -> int

val return : 'a -> 'a t
val create : unit -> 'a t
val push : 'a t -> 'a -> unit
val init : int -> (int -> 'a) -> 'a t

val get : 'a t -> int -> 'a
val set : 'a t -> int -> 'a -> unit
val map : ('a -> 'b) -> 'a t -> 'b t
val flat_map_seq : ('a -> 'b sequence) -> 'a t -> 'b t
val fold : ('a -> 'b -> 'a) -> 'a -> 'b t -> 'a
val iteri : f:(int -> 'a -> unit) -> 'a t -> unit
val append_seq : 'a t -> 'a sequence -> unit

val of_seq : 'a sequence -> 'a t
val of_list : 'a list -> 'a t
val of_array : 'a array -> 'a t

val to_seq : 'a t -> 'a sequence
val to_list : 'a t -> 'a list
val to_array : 'a t -> 'a array
