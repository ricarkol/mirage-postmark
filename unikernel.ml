open Lwt.Infix
open Mirage_fs
open Mirage_types_lwt

module Main (Time: TIME)(B: BLOCK) = struct
  let log_src = Logs.Src.create "block" ~doc:"block tester"
  module Test = Fat.FS(Block)
  module Log = (val Logs.src_log log_src : Logs.LOG)

(*
set size 1000 9000
set number 50000
set transactions 100000
run
*)

  let pm_low = 100
  let pm_hi = 900
  let pm_number = 5000
  let pm_transactions = 10000
  let pm_bias_read = 5
  let pm_bias_create = 5
  let pm_directories = 100

  let pm_last_index = ref 0

  let rec iter_s f = function
    | [] -> Lwt.return (Result.Ok ())
    | x :: xs ->
      f x >>= function
      | Result.Error e -> Lwt.return (Result.Error e)
      | Result.Ok () -> iter_s f xs

  let alloc_bytes bytes =
    let pages = Io_page.(to_cstruct (get ((bytes + 4095) / 4096))) in
    let phrase = "All work and no play makes Dave a dull boy.\n" in
    let sector = Cstruct.sub pages 0 bytes in
    for i = 0 to Cstruct.len sector - 1 do
      Cstruct.set_char sector i phrase.[i mod (String.length phrase)]
    done;
    sector

  let ( >>*= ) x f = x >>= function
    | Error _ -> Lwt.fail (Failure "error")
    | Ok x -> f x

  open Mirage_block

  let start _time b () =
    B.get_info b >>= fun info ->
    Log.info (fun f -> f "sectors = %Ld\nread_write=%b\nsector_size=%d\n%!"
      info.size_sectors info.read_write info.sector_size);
    Block.connect "disk.img" >>= fun device ->
    Test.connect device >>= fun fs ->
  let open Test in
  let module Path = Fat_path in
  let cwd = ref (Path.of_string "/") in

  let do_dir dir =
    let path = Path.cd !cwd dir in
    stat fs (Path.to_string path) >>*= function
    | { directory = true; _ } ->
      let file = Path.to_string path in
      listdir fs file >>*= fun xs ->
      Printf.printf "Directory for A:%s\n\n" (Path.to_string path);
      List.iter (fun x -> Printf.printf "%s\n" x) xs;
      Printf.printf "%9d files\n%!" (List.length xs);
      Lwt.return ()
    | { directory = false; _ } ->
      Printf.printf "Not a directory.\n%!";
      Lwt.return ()
  in

  let do_type file =
    let path = Path.cd !cwd file in
    stat fs (Path.to_string path) >>*= function
    | { directory = true; _ } ->
      Printf.printf "Is a directory.\n%!";
      Lwt.return ()
    | { directory = false; size; _ } ->
      let file_size = Int64.to_int size in
      read fs (Path.to_string path) 0 file_size >>*= fun datas ->
      let n = ref 0 in
      List.iter (fun buf ->
          Printf.printf "%s" (Cstruct.to_string buf);
          n := !n + (Cstruct.len buf)
        ) datas;
      Printf.printf "\n%!";
      if !n <> file_size
      then Printf.printf "Short read; expected %d got %d\n%!" file_size !n;
      Lwt.return ()
  in

  let do_del file =
    let _ = Log.info (fun f -> f "del %s\n" file) in
    let path = Path.cd !cwd file in
    destroy fs (Path.to_string path) >>*= fun () -> Lwt.return ()
  in

  let do_cd dir =
    let path = Path.cd !cwd dir in
    stat fs (Path.to_string path) >>*= function
    | { directory = true; _ } ->
      cwd := path;
      Lwt.return ()
    | { directory = false; _ } ->
      Printf.printf "Not a directory.\n%!";
      Lwt.return ()
  in

  let do_write x offset size =
    let _ = Log.info (fun f -> f "write %s\n" x) in
    let path = Path.cd !cwd x in
    let buf = alloc_bytes size in
    write fs (Path.to_string path) offset buf >>*= fun () -> Lwt.return ()
  in

  let do_read x size =
    let _ = Log.info (fun f -> f "read %s\n" x) in
    let path = Path.cd !cwd x in
    read fs (Path.to_string path) 0 size >>*= fun _ -> Lwt.return ()
  in

  let do_touch x =
    let _ = Log.info (fun f -> f "touch %s\n" x) in
    let path = Path.cd !cwd x in
    create fs (Path.to_string path) >>*= fun () -> Lwt.return ()
  in

  let do_mkdir x =
    let path = Path.cd !cwd x in
    mkdir fs (Path.to_string path) >>*= fun () -> Lwt.return ()
  in

  let do_rmdir x =
    let path = Path.cd !cwd x in
    destroy fs (Path.to_string path) >>*= fun () -> Lwt.return ()
  in

  let file_table = Array.make (pm_number+pm_transactions) "" in
  let file_size_table = Array.make (pm_number+pm_transactions) 0 in

  let create_subdirs () =
    let rec loop = function
      | -1 -> Lwt.return_unit
      | n ->
         do_mkdir (Printf.sprintf "s%d" n) >>= fun () ->
         loop (n-1)
     in
     loop (pm_directories - 1)
  in

  let create_files () =
    pm_last_index := pm_number;
    let rec loop = function
      | -1 -> Lwt.return_unit
      | n ->
         file_table.(n) <- Printf.sprintf "s%d/%d" (Random.int pm_directories) n;
         file_size_table.(n) <- pm_low + (Random.int (pm_hi-pm_low));
         do_touch file_table.(n) >>= fun () ->
         do_write file_table.(n) 0 file_size_table.(n) >>= fun () ->
         loop (n-1)
     in
     loop (pm_number - 1)
  in

  create_subdirs () >>= fun () ->
  create_files () >>= fun () ->

  let pm_append n =
    let block = Random.int (pm_hi-file_size_table.(n))+1 in 
    if file_size_table.(n) < pm_hi then
      do_write file_table.(n) file_size_table.(n) block
    else
      Lwt.return ()
  in

  let pm_read n =
    do_read file_table.(n) file_size_table.(n)
  in

  let pm_create nn =
    let n = !pm_last_index+1 in
    pm_last_index := !pm_last_index + 1;
    file_table.(n) <- Printf.sprintf "s%d/%d" (Random.int pm_directories) n;
    file_size_table.(n) <- pm_low + (Random.int (pm_hi-pm_low));
    do_touch file_table.(n) >>= fun () ->
    do_write file_table.(n) 0 file_size_table.(n)
  in

  let pm_delete n =
    let f = file_table.(n) in
    file_table.(n) <- "";
    file_size_table.(n) <- 0;
    do_del f
  in

  let rec pm_get_used_file n =
      let parse_size = function
        | 0 -> pm_get_used_file (Random.int !pm_last_index)
        | _ -> n
      in
      parse_size file_size_table.(n)
  in

  let pm_transaction n =
    let _ = if (Random.int 10) < pm_bias_read then pm_read n else pm_append n in
    if (Random.int 10) < pm_bias_create then pm_create n else pm_delete n
  in

  let rec loop = function
    | 0 -> Lwt.return_unit
    | n ->
      let fn = pm_get_used_file (Random.int !pm_last_index) in
      pm_transaction fn >>= fun () ->
      loop (n-1)
  in
  loop pm_transactions

end
