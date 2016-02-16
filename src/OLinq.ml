
(* This file is free software, part of OLinq. See file "license" for more details. *)

(** {1 LINQ-like operations on collections} *)

type 'a sequence = ('a -> unit) -> unit
type 'a equal = 'a -> 'a -> bool
type 'a ord = 'a -> 'a -> int
type 'a hash = 'a -> int
type 'a or_error = [`Ok of 'a | `Error of string ]

let id_ x = x

module M = OLinq_map

type 'a search_result =
  | SearchContinue
  | SearchStop of 'a

type ('a,'b,'key,'c) join_descr = {
  join_key1 : 'a -> 'key;
  join_key2 : 'b -> 'key;
  join_merge : 'key -> 'a -> 'b -> 'c option;
  join_build_src : 'key M.Build.src;
}

type ('a,'b) group_join_descr = {
  gjoin_proj : 'b -> 'a;
  gjoin_build_src : 'a M.Build.src;
}

type ('a,'b) search_descr = {
  search_check: ('a -> 'b search_result);
  search_failure : 'b;
}

module ImplemSetOps = struct
  let choose s = Sequence.take 1 s

  let distinct ~cmp s = Sequence.sort_uniq ~cmp s

  let search obj s =
    match
      Sequence.find
        (fun x -> match obj.search_check x with
           | SearchContinue -> None
           | SearchStop y -> Some y)
        s
    with None -> obj.search_failure
       | Some x -> x

  let do_join ~join c1 c2 =
    let build1 =
      let seq = Sequence.map (fun x -> join.join_key1 x, x) c1 in
      M.of_seq ~src:join.join_build_src seq
    in
    let l =
      Sequence.fold
        (fun acc y ->
           let key = join.join_key2 y in
           match M.get build1 key with
           | None -> acc
           | Some l1 ->
               List.fold_left
                 (fun acc x -> match join.join_merge key x y with
                    | None -> acc
                    | Some res -> res::acc)
                 acc l1)
        [] c2
    in
    Sequence.of_list l

  let do_group_join ~gjoin c1 c2 =
    let build = M.Build.of_src gjoin.gjoin_build_src in
    c1 (fun x -> M.Build.add build x []);
    c2
      (fun y ->
         (* project [y] into some element of [c1] *)
         let x = gjoin.gjoin_proj y in
         M.Build.update build x ~or_:[] ~f:(fun l -> y::l));
    M.Build.get build

  let do_union ~src c1 c2 =
    let build = M.Build.of_src src  in
    c1 (fun x -> M.Build.add build x ());
    c2 (fun x -> M.Build.add build x ());
    let m = M.Build.get build in
    fun yield -> M.iter m (fun k _ -> yield k)

  type inter_status =
    | InterLeft
    | InterDone  (* already output *)

  let do_inter ~src c1 c2 =
    let build = M.Build.of_src src in
    let l = ref [] in
    c1 (fun x -> M.Build.add build x InterLeft);
    c2 (fun x ->
        M.Build.update build x
          ~or_:InterDone
          ~f:(function
            | InterDone -> InterDone
            | InterLeft -> l := x :: !l; InterDone)
      );
    Sequence.of_list !l

  let do_diff ~src c1 c2 =
    let build = M.Build.of_src src in
    c2 (fun x -> M.Build.add build x ());
    let map = M.Build.get build in
    (* output elements of [c1] not in [map] *)
    Sequence.filter (fun x -> not (M.mem map x)) c1

  let do_subset ~src c1 c2 =
    let build = M.Build.of_src src in
    c2 (fun x -> M.Build.add build x ());
    let map = M.Build.get build in
    let res = Sequence.for_all (M.mem map) c1 in
    Sequence.return res
end

(** {2 Query operators} *)

type (_, _) unary =
  | Map : ('a -> 'b) -> ('a, 'b) unary
  | Filter : ('a -> bool) -> ('a, 'a) unary
  | Fold : ('b -> 'a -> 'b) * 'b -> ('a, 'b) unary
  | Size : ('a, int) unary
  | Choose : ('a, 'a) unary
  | FilterMap : ('a -> 'b option) -> ('a, 'b) unary
  | FlatMap : ('a -> 'b sequence) -> ('a, 'b) unary
  | Take : int -> ('a, 'a) unary
  | TakeWhile : ('a -> bool) -> ('a, 'a) unary
  | Sort : 'a ord -> ('a, 'a) unary
  | SortBy : 'b ord * ('a -> 'b) -> ('a, 'a) unary
  | Distinct : 'a ord -> ('a, 'a) unary
  | Search : ('a, 'b) search_descr -> ('a, 'b) unary
  | Contains : 'a equal * 'a -> ('a, bool) unary
  | GroupBy :
      'b M.Build.src
      * ('a -> 'b)
      -> ('a, ('b, 'a list) M.t) unary
  | Count : 'a M.Build.src -> ('a, ('a, int) M.t) unary
  | Lazy : ('a lazy_t, 'a) unary

type (_,_) set_op =
  | Union : ('a,'a) set_op
  | Inter : ('a,'a) set_op
  | Diff : ('a,'a) set_op
  | Subset : ('a, bool) set_op

type (_, _, _) binary =
  | App : ('a -> 'b, 'a, 'b) binary
  | Join :
      ('a, 'b, 'key, 'c) join_descr
      -> ('a, 'b, 'c) binary
  | GroupJoin :
      ('a, 'b) group_join_descr
      -> ('a, 'b, ('a, 'b list) M.t) binary
  | Product : ('a, 'b, ('a*'b)) binary
  | Append : ('a, 'a, 'a) binary
  | SetOp :
      ('a, 'b) set_op * 'a M.Build.src
      -> ('a, 'a, 'b) binary

(* TODO deal with several possible containers as a 'a t,
   including seq,vector, M... *)

(* type of queries that return values of type ['a] *)
type 'a t_ =
  | Return : 'a -> 'a t_
  | OfSeq : 'a sequence -> 'a t_
  | Unary : ('a, 'b) unary * 'a t_ -> 'b t_
  | Binary : ('a, 'b, 'c) binary * 'a t_ * 'b t_ -> 'c t_
  | Bind : ('a -> 'b t_) * 'a t_ -> 'b t_
  | Reflect : 'a t_ -> 'a sequence t_

(* type of queries, with an additional phantom parameter *)
type ('a, 'card) t = 'a t_ constraint 'card = [<`One | `AtMostOne | `Any]

type 'a t_any = ('a, [`Any]) t
type 'a t_one = ('a, [`One]) t
type 'a t_at_most_one = ('a, [`AtMostOne]) t

let of_list l =
  OfSeq (Sequence.of_list l)

let of_array a =
  OfSeq (Sequence.of_array a)

let of_array_i a =
  OfSeq (Sequence.of_array_i a)

let of_hashtbl h =
  OfSeq (Sequence.of_hashtbl h)

let range i j = OfSeq (Sequence.int_range ~start:i ~stop:j)

let (--) = range

let of_seq seq = OfSeq seq

let of_queue q = OfSeq (Sequence.of_queue q)

let of_stack s = OfSeq (Sequence.of_stack s)

let of_string s = OfSeq (Sequence.of_str s)

let of_map m = OfSeq (M.to_seq m)

let of_multimap m = OfSeq (M.to_seq_multimap m)

(** {6 Execution} *)

(* apply a unary operator on a collection *)
let _do_unary : type a b. (a,b) unary -> a sequence -> b sequence
  = fun u c -> match u with
    | Map f -> Sequence.map f c
    | Filter p -> Sequence.filter p c
    | Fold (f, acc) -> Sequence.return (Sequence.fold f acc c)
    | Size -> Sequence.return (Sequence.length c)
    | Choose -> ImplemSetOps.choose c
    | FilterMap f -> Sequence.filter_map f c
    | FlatMap f -> Sequence.flat_map f c
    | Take n -> Sequence.take n c
    | TakeWhile p -> Sequence.take_while p c
    | Sort cmp -> Sequence.sort ~cmp c
    | SortBy (cmp,proj) -> Sequence.sort ~cmp:(fun a b -> cmp (proj a) (proj b)) c
    | Distinct cmp -> ImplemSetOps.distinct ~cmp c
    | Search obj -> Sequence.return (ImplemSetOps.search obj c)
    | GroupBy (src,f) ->
        let seq = Sequence.map (fun x -> f x, x) c in
        Sequence.return (M.of_seq ~src seq)
    | Contains (eq, x) -> Sequence.return (Sequence.mem ~eq x c)
    | Count src ->
        Sequence.return (M.count_seq ~src c)
    | Lazy -> Sequence.map Lazy.force c

let _do_binary : type a b c. (a, b, c) binary -> a sequence -> b sequence -> c sequence
  = fun b c1 c2 -> match b with
    | Join join -> ImplemSetOps.do_join ~join c1 c2
    | GroupJoin gjoin -> Sequence.return (ImplemSetOps.do_group_join ~gjoin c1 c2)
    | Product -> Sequence.product c1 c2
    | Append -> Sequence.append c1 c2
    | App -> Sequence.(c1 <*> c2)
    | SetOp (Inter,src) -> ImplemSetOps.do_inter ~src c1 c2
    | SetOp (Union,src) -> ImplemSetOps.do_union ~src c1 c2
    | SetOp (Diff,src) -> ImplemSetOps.do_diff ~src c1 c2
    | SetOp (Subset,src) -> ImplemSetOps.do_subset ~src c1 c2

let rec _run : type a. a t_ -> a sequence
  = fun q -> match q with
    | Return c -> Sequence.return c
    | Unary (u, q') -> _do_unary u (_run q')
    | Binary (b, q1, q2) -> _do_binary b (_run q1) (_run q2)
    | OfSeq s -> s
    | Bind (f, q') ->
        let seq = _run q' in
        Sequence.flat_map
          (fun x ->
             let q'' = f x in
             _run q'')
          seq
    | Reflect q ->
        let seq = Sequence.persistent_lazy (_run q) in
        Sequence.return seq

let _apply_limit ?limit seq = match limit with
  | None -> seq
  | Some l -> Sequence.take l seq

(* safe execution *)
let run ?limit q =
  let seq = _run q in
  _apply_limit ?limit seq

let run1_exn q =
  let seq = _run q in
  match Sequence.head seq with
  | Some x -> x
  | None -> raise Not_found

let run1 q =
  let seq = _run q in
  match Sequence.head seq with
  | Some x -> x
  | None -> assert false (* phantom type *)

let run_list ?limit q = run ?limit q |> Sequence.to_list

let run_array ?limit q = run ?limit q |> Sequence.to_array

(** {6 Basics} *)

let empty = OfSeq Sequence.empty

let rec map
: type a b. (a -> b) -> a t_ -> b t_
= fun f q -> match q with
  | Binary (Append, q1, q2) -> Binary (Append, map f q1, map f q2)
  | Unary (Map f', q) -> map (fun x -> f (f' x)) q
  | Unary (Filter p, q) ->
      filter_map (fun x -> if p x then Some (f x) else None) q
  | _ -> Unary (Map f, q)

and filter_map
: type a b. (a -> b option) -> a t_ -> b t_
= fun f q -> match q with
  | Unary (Map f', q) -> filter_map (fun x -> f (f' x)) q
  | _ -> Unary (FilterMap f, q)

let (>|=) q f = map f q

let rec filter
: type a. (a -> bool) -> a t_ -> a t_
= fun p q -> match q with
  | Binary (Append, q1, q2) -> Binary (Append, filter p q1, filter p q2)
  | _ -> Unary (Filter p, q)

let flat_map_seq f q = Unary (FlatMap f, q)

let flat_map_l f q =
  let f' x = Sequence.of_list (f x) in
  flat_map_seq f' q

let flatten_seq q = flat_map_seq id_ q

let flatten_list q = flat_map_seq Sequence.of_list q

let flatten_map q = flat_map_seq M.to_seq q

let flatten_multimap q = flat_map_seq M.to_seq_multimap q

let rec take
: type a. int -> a t_ -> a t_
= fun n q -> match q with
  | Unary (Map f, q) -> map f (take n q)
  | _ -> Unary (Take n, q)

let take1 q = take 1 q

let take_while p q = Unary (TakeWhile p, q)

let sort ?(cmp=Pervasives.compare) () q = Unary (Sort cmp, q)

let sort_by ?(cmp=Pervasives.compare) proj q = Unary (SortBy (cmp, proj), q)

let distinct ?(cmp=Pervasives.compare) () q = Unary (Distinct cmp, q)

let group_by ?cmp ?eq ?hash f q =
  let src = M.Build.src_of_args ?cmp ?eq ?hash () in
  Unary (GroupBy (src, f), q)

let group_by' ?cmp ?eq ?hash f q =
  flat_map_seq M.to_seq (group_by ?cmp ?eq ?hash f q)

let count ?cmp ?eq ?hash () q =
  let src = M.Build.src_of_args ?cmp ?eq ?hash () in
  Unary (Count src, q)

let count' ?cmp ?eq ?hash () q =
  flat_map_seq M.to_seq (count ?cmp ?eq ?hash () q)

let rec fold
: type a b. (a -> b -> a) -> a -> b t_ -> a t_
= fun f acc q -> match q with
  | Unary (Map f', q) -> fold (fun acc x -> f acc (f' x)) acc q
  | _ -> Unary (Fold (f, acc), q)

let rec size
: type a. a t_ -> int t_
= function
  | Unary (Choose, _) -> Return 1
  | Unary (Sort _, q) -> size q
  | Unary (SortBy _, q) -> size q
  | Unary (Map _, q) -> size q
  | q -> Unary (Size, q)

let sum q = Unary (Fold ((+), 0), q)

let _lift_some f x y = match y with
  | None -> Some x
  | Some y -> Some (f x y)

let max q = Unary (Fold (Pervasives.max, min_int), q)
let min q = Unary (Fold (Pervasives.min, max_int), q)
let average q =
  q
  |> fold (fun (sum,num) x -> x+sum, num+1) (0,0)
  |> map (fun (sum,num) -> sum/num)

let is_empty q =
  Unary
    (Search {
      search_check = (fun _ -> SearchStop false); (* stop in case there is an element *)
      search_failure = true;
    }, q)

let contains ?(eq=(=)) x q =
  Unary (Contains (eq, x), q)

let for_all p q =
  Unary
    (Search {
      search_check = (fun x -> if p x then SearchContinue else SearchStop false);
      search_failure = true;
    }, q)

let exists p q =
  Unary
    (Search {
      search_check = (fun x-> if p x then SearchStop true else SearchContinue);
      search_failure = false;
    }, q)

let find p q =
  Unary
    (Search {
      search_check = (fun x -> if p x then SearchStop (Some x) else SearchContinue);
      search_failure = None;
    }, q)

let find_map f q =
  Unary
    (Search {
      search_check =
        (fun x -> match f x with
          | Some y -> SearchStop (Some y)
          | None -> SearchContinue);
      search_failure = None;
    }, q)

(** {6 Binary Operators} *)

let join ?cmp ?eq ?hash join_key1 join_key2 ~merge q1 q2 =
  let join_build_src = M.Build.src_of_args ?eq ?hash ?cmp () in
  let j = {
    join_key1;
    join_key2;
    join_merge=merge;
    join_build_src;
  } in
  Binary (Join j, q1, q2)

let group_join ?cmp ?eq ?hash gjoin_proj q1 q2 =
  let gjoin_build_src = M.Build.src_of_args ?eq ?hash ?cmp () in
  let j = {
    gjoin_proj;
    gjoin_build_src;
  } in
  Binary (GroupJoin j, q1, q2)

let group_join' ?cmp ?eq ?hash gjoin_proj q1 q2 =
  let q = group_join ?cmp ?eq ?hash gjoin_proj q1 q2 in
  flat_map_seq M.to_seq q

let product q1 q2 = Binary (Product, q1, q2)

let append q1 q2 = Binary (Append, q1, q2)

let inter ?cmp ?eq ?hash () q1 q2 =
  let build = M.Build.src_of_args ?cmp ?eq ?hash () in
  Binary (SetOp (Inter, build), q1, q2)

let union ?cmp ?eq ?hash () q1 q2 =
  let build = M.Build.src_of_args ?cmp ?eq ?hash () in
  Binary (SetOp (Union, build), q1, q2)

let diff ?cmp ?eq ?hash () q1 q2 =
  let build = M.Build.src_of_args ?cmp ?eq ?hash () in
  Binary (SetOp (Diff, build), q1, q2)

let subset ?cmp ?eq ?hash () q1 q2 =
  let build = M.Build.src_of_args ?cmp ?eq ?hash () in
  Binary (SetOp (Subset, build), q1, q2)

let map_fst f q = map (fun (x,y) -> f x, y) q
let map_snd f q = map (fun (x,y) -> x, f y) q

let flatten_opt q = filter_map id_ q

exception UnwrapNone

let opt_unwrap_exn q =
  Unary
    (Map
       (function
         | Some x -> x
         | None -> raise UnwrapNone),
     q)

(** {6 Applicative} *)

let pure x = Return x

let app f x = match f, x with
  | Return f, Return x -> Return (f x)
  | Return f, _ -> map f x
  | f, Return x -> map (fun f -> f x) f
  | _ -> Binary (App, f, x)

let (<*>) = app

(** {6 Monadic stuff} *)

let return x = Return x

let flat_map f q = Bind (f,q)

let (>>=) x f = Bind (f, x)

(** {6 Misc} *)

let lazy_ q = Unary (Lazy, q)

(** {6 Others} *)

let rec choose
: type a. a t_ -> a t_
= function
  | Unary (Map f, q) -> map f (choose q)
  | Unary (Lazy, q) -> Unary (Lazy, choose q)
  | Binary (Product, q1, q2) ->
      let q1 = choose q1 and q2 = choose q2 in
      app (map (fun x y -> x,y) q1) q2
  | Binary (App, f, x) ->
      let q_f = choose f and q_x = choose x in
      app (map (fun f x -> f x) q_f) q_x
  | Unary (Fold _, _) as q -> q (* one solution *)
  | q -> Unary (Choose, q)

(** {6 Infix} *)

module Infix = struct
  let (>>=) = (>>=)
  let (>|=) = (>|=)
  let (<*>) = (<*>)
  let (--) = (--)
end

(** {6 Adapters} *)

let reflect_seq q = Reflect q

let reflect_list q = Unary (Map Sequence.to_list, Reflect q)

let reflect_hashtbl q =
  Unary (Map (fun c -> Sequence.to_hashtbl c), Reflect q)

let reflect_queue q =
  Unary (Map (fun c -> let q = Queue.create() in Sequence.to_queue q c; q), Reflect q)

let reflect_stack q =
  Unary (Map (fun c -> let s = Stack.create () in Sequence.to_stack s c; s), Reflect q)

module AdaptSet(S : Set.S) = struct
  let of_set set = OfSeq (fun k -> S.iter k set)

  let reflect q =
    let f c = Sequence.fold (fun set x -> S.add x set) S.empty c in
    map f (reflect_seq q)

  let run q = run1 (reflect q)
end

module AdaptMap(M : Map.S) = struct
  let _to_seq m k = M.iter (fun x y -> k (x,y)) m

  let of_map map = OfSeq (_to_seq map)

  let reflect q =
    let f c =
      Sequence.fold (fun m (x,y) -> M.add x y m) M.empty c
    in
    map f (reflect_seq q)

  let run q = run1 (reflect q)
end

module IO = struct
  let read_all_ ~size ic =
    let buf = ref (Bytes.create size) in
    let len = ref 0 in
    try
      while true do
        (* resize *)
        if !len = Bytes.length !buf then (
          buf := Bytes.extend !buf 0 !len;
        );
        assert (Bytes.length !buf > !len);
        let n = input ic !buf !len (Bytes.length !buf - !len) in
        len := !len + n;
        if n = 0 then raise Exit;  (* exhausted *)
      done;
      assert false (* never reached*)
    with Exit -> Bytes.sub_string !buf 0 !len

  let slurp_ with_input =
    let l = lazy (with_input (fun ic -> read_all_ ~size:2048 ic)) in
    lazy_ (return l)

  let read_chan ic = slurp_ (fun f -> f ic)

  let finally_ f x ~h =
    try
      let res = f x in
      h();
      res
    with e ->
      h();
      raise e

  let with_in filename f =
    let ic = open_in filename in
    finally_ f ic ~h:(fun () -> close_in ic)

  let with_out filename f =
    let oc = open_out filename in
    finally_ f oc ~h:(fun () -> close_out oc)

  let read_file filename = slurp_ (with_in filename)

  (* find [c] in [s], starting at offset [i] *)
  let rec _find s c i =
    if i >= String.length s then None
    else if s.[i] = c then Some i
    else _find s c (i+1)

  let rec _lines s i k = match _find s '\n' i with
    | None ->
        if i<String.length s then k (String.sub s i (String.length s-i))
    | Some j ->
        let s' = String.sub s i (j-i) in
        k s';
        _lines s (j+1) k

  let lines q =
    (* sequence of lines *)
    let f s = _lines s 0 in
    flat_map_seq f q

  let lines_l q =
    let f s = lazy (Sequence.to_list (_lines s 0)) in
    lazy_ (map f q)

  let _join ~sep ?(stop="") seq =
    let buf = Buffer.create 128 in
    Sequence.iteri
      (fun i x ->
         if i>0 then Buffer.add_string buf sep;
         Buffer.add_string buf x)
      seq;
    Buffer.add_string buf stop;
    Buffer.contents buf

  let unlines q =
    let f l = lazy (_join ~sep:"\n" ~stop:"\n" l) in
    lazy_ (map f (reflect_seq q))

  let join sep q =
    let f l = lazy (_join ~sep l) in
    lazy_ (map f (reflect_seq q))

  let out oc q =
    output_string oc (run1 q)

  let out_lines oc q =
    let x = run q in
    Sequence.iter (fun l -> output_string oc l; output_char oc '\n') x

  let to_file_exn filename q =
    with_out filename (fun oc -> out oc q)

  let to_file filename q =
    try `Ok (with_out filename (fun oc  -> out oc q))
    with Failure s -> `Error s

  let to_file_lines_exn filename q =
    with_out filename (fun oc -> out_lines oc q)

  let to_file_lines filename q =
    try `Ok (with_out filename (fun oc  -> out_lines oc q))
    with Failure s -> `Error s
end
