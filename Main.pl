use DBI;
use Digest::MD5;
use POSIX qw(strftime);
use Getopt::Std;
#use GnuPG;
use Term::ReadKey;
use File::Path;
#use Config::Properties;

# Global vars
our $nodb; # Object used to store the connection to the DB
our $passphrase; # Will store the passphrase for the GPG Key
our %config; # Configuration loaded from "Config.pl" (Hardcoded for the moment)
our %opts;
#our $envFileSep = System.getProperty("path.separator");
our $envFileSep = "/";

# Declare the subroutine that will be used below.
sub Main; # Will be called in every situation
sub Init; # Will be used in case we need to create the initial setup (db mainly)
sub printHelp; # Will print help in case user send bad options
sub Normal; # Will be used in normal running (Init already done)
sub Normal1Dir; # Is used by Normal to do the jobs for one dir
sub Normal1File; # Is used by Normal1Dir to work on a specific file
sub InsertFile; # Is used to handle a new file
sub AddFileInRepository; # Is used to crypt a file into a repository and update the repository
sub CreateNewDirInRepo; # Is used to create a new directory in the repository
sub InsertFileIntoDB; # Is used to insert a file in the DB (usually called by InsertFile)
sub UpdateFile; # Is used to update a file
sub UpdateFileInDB; # Is used to update an entry each time the checksum is modified  (usually called by UpdateFile)
sub printVerbose;
sub printMsgDB;
sub printMsgL1;
sub printMsgSystem;
sub SecureDB; # At the end, securize database
sub UnsecureDB; # At the beginning, use the existing DB

getopts('gn', \%opts) or printHelp;
Main;

sub Init () {
	my @dbCreationStatements;

	$dbCreationStatements[0]="";
	$dbCreationStatements[1]="CREATE TABLE FILES (ID_File integer primary key autoincrement, ID_Directory integer, Filename text, HASH varchar(32), ID_Backup integer);";
	$dbCreationStatements[2]="CREATE TABLE REPOFILES (ID_HASH integer primary key autoincrement, HASH varchar(32), Counter integer);";
	$dbCreationStatements[3]="CREATE TABLE REPODIRS (ID_Dir integer primary key autoincrement, Counter integer);";
	$dbCreationStatements[4]="CREATE TABLE BACKUPS (ID_Backup integer primary key autoincrement, Date text, Comment text);";
	$dbCreationStatements[5]="CREATE TABLE DIRECTORIES (ID_Dir integer primary key autoincrement, DirName text);";

	my $req;
	while (defined ($req = shift @dbCreationStatements)) {
		my $link = $nodb->prepare($dbCreationStatements[0]);
		$link->execute();
	}
	
	CreateNewDirInRepo 1;
}

sub UnsecureDB {
	my $dbDecCmd = "gpg --decrypt -o ".$config{'sqldb'}." ".$config{'tgtdir'}.$envFileSep.$config{'sqlEncDb'}.".gpg";
	printMsgL1 "Unsecuring DB.\n";
	my $ret = system($dbDecCmd);
	printMsgL1 "Return code in UnsecureDB : ".$ret;
	
	if ($ret != 0) {
		die ("Error while extracting DB. Please check your db.\n");	
	}
	
}

sub SecureDB {
	unlink $config{'tgtdir'}.$envFileSep.$config{'sqlEncDb'}.".gpg";
	my $dbEncCmd = "gpg -se -r ".$config{'gpgKeyId'}." -u ".$config{'gpgKeyId'}." -o ".$config{'tgtdir'}.$envFileSep.$config{'sqlEncDb'}.".gpg ".$config{'sqldb'};
	printMsgL1 "Securing DB.\n";
	my $ret = system($dbEncCmd);
	printMsgL1 "Return code in SecureDB : ".$ret;
	
	if ($ret != 0) {
		die ("Error while securing DB. Please check your db.\n");
	}

	printMsgL1 "Removing unsecured DB.\n";
	unlink $config{'sqldb'};
}

sub printVerbose {
	print $_[0]."\n";
}

sub printMsgDB {
	#printVerbose "DB Request : ".$_[0];
}

sub printMsgL1 {
	#printVerbose "Verbosity 1 Msg : ".$_[0];
}

sub printMsgSystem {
	# Do whatever you want
}


sub Normal {
	my $idBackup;

	# Initiate a new backup : Create a new one in DB
	my $bakDate = strftime "%Y/%m/%d %I:%M:%S %p", localtime;
	my $dbReqNewBackup = "INSERT INTO BACKUPS('Date') VALUES('".$bakDate."');";
	my $linkNewBackup = $nodb->prepare($dbReqNewBackup);
	$linkNewBackup->execute;

	# Get Current ID_Backup
	my $dbReqGetIdBackup = "SELECT seq FROM sqlite_sequence WHERE name='BACKUPS';";
	my $linkGetIdBackup = $nodb->prepare($dbReqGetIdBackup);
	$linkGetIdBackup->execute;
	$idBackup = $linkGetIdBackup->fetch->[0];

	# RUN the operations
	while (defined (my $lDir = shift $config{srcdir})) {
		Normal1Dir $lDir, $idBackup;
		$idDir++;
	}
}

sub Normal1Dir  {
	my $wDir = $_[0]; # Argument 1 is SourceDirectory
	my $wIdBackup = $_[1]; # Argument 2 is Backup ID

	my $wIdDir; # Should be initialized by checking in DB. To be corrected

	# Insert if not present the directory to backup
	my $dbCheckDir = "SELECT ID_Dir FROM DIRECTORIES WHERE DirName='".$wDir."';";
	my $linkCheckDir = $nodb->prepare($dbCheckDir);
	$linkCheckDir->execute;
	if (! defined (my $rowCheckDir = $linkCheckDir->fetch)) {
		# INSERT INTO DB
		my $dbInsertDir = "INSERT INTO DIRECTORIES('DirName') VALUES('".$wDir."');";
		my $linkInsertDir = $nodb->prepare($dbInsertDir);
		$linkInsertDir->execute;
		my $linkReCheckDir = $nodb->prepare($dbCheckDir);
		$linkReCheckDir->execute;
		$wIdDir=$linkReCheckDir->fetch->[0];
	} else {
		$wIdDir= $rowCheckDir->[0];
	}
	
	chdir($wDir);
	my %listfiles = `find . -type f -print`;
	
	# Create the target directory 
	mkpath ($config{'tgtdir'}.$envFileSep.$config{'dirprefix'}.$wIdDir.$envFileSep.$wIdBackup);
		
	foreach my $v (values(%listfiles)) {
		chomp($v);
		stat($v);
		if (-f _ && ! -z _) {
			Normal1File $v, $wDir, $wIdBackup, $wIdDir;
		} else {
			print "NO : '$v' will not be backed up.\n";
		}
	}
}

sub Normal1File {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $wIdBackup = $_[2]; # Argument 3 is Backup ID
	my $wIdDir = $_[3]; # Argument 4 is Directory ID

	open (FILE, $filename);
	binmode(FILE);
	my $md5Checksum	= Digest::MD5->new;
	while (<FILE>) {
		$md5Checksum->add($_);
	}
	close FILE;

	# Test if file is already in the table
	my $dbReqLookFor = "SELECT HASH from FILES where Filename='".$filename."' AND ID_Directory = ".$wIdDir.";";
	my $linkReqLookFor = $nodb->prepare($dbReqLookFor);
	$linkReqLookFor->execute;
	if (my $rowReqLookFor = $linkReqLookFor->fetch) {
		my $tHash = $rowReqLookFor->[0]; 
		if ($md5Checksum->hexdigest != $tHash) {
			UpdateFile $filename, $wDir, $wIdBackup, $md5Checksum->hexdigest;
		} else {
			#print "Same Checkshum. \n";			
		}
	} else {
		#print "NEW : '$filename is new. Inserting in DB.\n";
		InsertFile $filename, $wDir, $wIdBackup, $wIdDir, $md5Checksum->hexdigest;
	}
}

sub InsertFile {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $wIdBackup = $_[2]; # Argument 3 is Backup ID
	my $wIdDir = $_[3]; # Argument 4 is Directory ID
	my $wChecksum = $_[4]; # Argument 5 is Checksum

	my $encryption = AddFileInRepository $filename, $wIdDir, $wChecksum, $wIdBackup, $wDir;
	#print "Encryption var : $encryption. ";
	if ($encryption == 0) {
		InsertFileIntoDB $filename, $wDir, $wIdBackup, $wIdDir, $wChecksum;	
	} else {
		my $tmpDir = $config{'tgtdir'}.$envFileSep.$config{'dirprefix'}.$wIdDir.$envFileSep.$wIdBackup;
		print "Will not insert $filename. Error while crypting to $wChecksum.gpg in $tmpDir\n";
	}
}

sub AddFileInRepository {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wIdDir = $_[1]; # Argument 2 is Backed up Directory ID
	my $wChecksum = $_[2]; # Argument 3 is Checksum
	my $wIdBackup = $_[3]; # Argument 4 is the backup ID
	my $wDir = $_[4]; # Argument 5 is the sourcefile directory

	my $returnValue = 0; # By default, everything will be fine :)

	# Get the the current counter of the hash
	printMsgL1 "Adding file '".$filename."' with HASH '".$wChecksum;
	my $dbReqGetHashCounter = "SELECT Counter FROM REPOFILES WHERE HASH='".$wChecksum."';";
	printMsgDB $dbReqGetHashCounter;
	my $linkGetHashCounter = $nodb->prepare($dbReqGetHashCounter);
	$linkGetHashCounter->execute;
	if (my $rowReqGetHashCounter = $linkGetHashCounter->fetch) {
		# Here, the HASH already exist in one subdir
		printMsgL1 "File is already in DB.";
		my $newCounter = $rowReqGetHashCounter->[0] + 1;
		my $dbReqUpdateCounter = "UPDATE REPOFILES SET Counter=".$newCounter." WHERE HASH='".$wChecksum."';";
		printMsgDB $dbReqUpdateCounter;
		my $linkUpdateCounter = $nodb->prepare($dbReqUpdateCounter);
		$linkUpdateCounter->execute;
	} else {
		# Here, the HASH does not exist at the moment. Let's create it
		printMsgL1 "File is not in repository";
		my $dbReqGetNbFilesInDir = "SELECT r.ID_Dir, r.Counter FROM REPODIRS as r, sqlite_sequence as s WHERE s.name='REPODIRS' AND s.seq=r.ID_Dir;";
		printMsgDB $dbReqGetNbFilesInDir;
		my $linkGetNbFilesInDir = $nodb->prepare($dbReqGetNbFilesInDir);
		$linkGetNbFilesInDir->execute;
		my $rowReqGetNbFilesInDir = $linkGetNbFilesInDir->fetch;
		my $currentDirId = $rowReqGetNbFilesInDir->[0];
		my $newDirCounter = $rowReqGetNbFilesInDir->[1] + 1;
		printMsgL1 "Current Dir ID : ".$currentDirId;
		printMsgL1 "Future Dir Counter : ".$newDirCounter;
		if ($newDirCounter > $config{'MaxFilesPerDirInRepo'}) {
			# Here, we need to create a new subdir
			$currentDirId++;
			CreateNewDirInRepo $currentDirId;
			$newDirCounter = 1;
		}

		# Define env for backup
		my $wDirBackup = $config{'tgtdir'}.$envFileSep.$currentDirId.$envFileSep;
		my $sourceFullFilename = $wDir.$envFileSep.$filename;
		my $targetEncSigFile = $wDirBackup.$envFileSep.$wChecksum.".gpg";
			
		# Encrypt file
		my $gpgEncCmd = "gpg -se -r ".$config{'gpgKeyId'}." -u ".$config{'gpgKeyId'}." -o '".$targetEncSigFile."' '".$sourceFullFilename."'";
		printMsgSystem $gpgEncCmd;
		$returnValue = system $gpgEncCmd;
		my $dbReqInsertFileHash = "INSERT INTO REPOFILES(HASH, Counter) VALUES ('".$wChecksum."', 1);";
		printMsgDB $dbReqInsertFileHash;
		my $linkInsertFileHash = $nodb->prepare($dbReqInsertFileHash);
		$linkInsertFileHash->execute;

		# , we update the count of files in the directory
		my $dbReqUpdateDirInRepo = "UPDATE REPODIRS SET Counter = ".$newDirCounter." WHERE ID_Dir='".$currentDirId."';";
		printMsgDB $dbReqUpdateDirInRepo;
		my $linkUpdateDirInRepo = $nodb->prepare($dbReqUpdateDirInRepo);
		$linkUpdateDirInRepo->execute;
	}
	return $returnValue;
}

sub CreateNewDirInRepo {
	printMsgL1 "New Directory Created. ID : ".$_[0];
	mkpath ($config{'tgtdir'}.$envFileSep.$_[0]);
	
	my $dbReqAddRepoDir = "INSERT INTO REPODIRS('Counter') VALUES (0);";
	printMsgDB $dbReqAddRepoDir;
	my $linkAddRepoDir = $nodb->prepare($dbReqAddRepoDir);
	$linkAddRepoDir->execute;
}

sub InsertFileIntoDB {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $wIdBackup = $_[2]; # Argument 3 is Backup ID
	my $wIdDir = $_[3]; # Argument 4 is Directory ID
	my $wChecksum = $_[4]; # Argument 5 is Checksum

	# Insert into DB as the file is not present at the moment
	my $dbReqInsert = "INSERT INTO FILES('ID_Directory', 'Filename', 'HASH', 'ID_Backup') VALUES(".$wIdDir.", \"".$filename."\", \"".$wChecksum."\", ".$wIdBackup.");";
	my $linkInsert = $nodb->prepare($dbReqInsert);
	$linkInsert->execute;
	
}

sub UpdateFile{
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $hexMD5 = $_[2]; # Argument 3 is MD5 Checksum

	# Create a new encrypted file
	
	# Remove the previous one

	# Finally update Database
	UpdateFileInDB $filename, $wDir, $hexMD5;
}

sub UpdateFileInDB {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $hexMD5 = $_[2]; # Argument 3 is MD5 Checksum

	my $dbReqUpdate = "UPDATE FILES SET HASH='".$hexMD5."' WHERE Filename='".$filename."' AND ID_Directory='".$wDir."';";
	my $linkUpdate = $nodb->prepare($dbReqUpdate);
	$linkUpdate->execute;
}

sub Main {
	
	%config = do "Config.pl";

	{
		my $testDBEncPath = $config{'tgtdir'}.$envFileSep.$config{'sqlEncDb'}.".gpg";		
		if (defined $opts{'n'}) {
			stat($testDBEncPath);
			if (-f _) {
				die ("Encrypted database already exist. Check if normal and remove option '-n'");
			}
		} else {
			stat($testDBEncPath);
			if (-f _) {
				# Get DB from repository
				UnsecureDB;
			} else {
				die ("You should specify option '-n' as specified Encrypted database does not exist");
			}
		}
	}

	# Initialize DB Connection
	$nodb = DBI->connect("dbi:SQLite:dbname=".$config{sqldb},"","") or die ("Error DB");
	if (defined $opts{'n'}) {		
		Init;
	}
	
	# Get Password (no echo)
	if (defined $opts{'g'}) {
		print "Type your password: ";
		ReadMode('noecho'); # don't echo
		chomp($passphrase = <STDIN>);
		ReadMode(0);        # back to normal
		print "\n";
	}
	
	Normal; # Let's go
	$nodb->disconnect;
	
	# Secure the DB
	SecureDB;
}

sub printHelp {
	print "-n : init a new database scheme. Warning : using this option on a already initialized db will make the program fail.\n";
	# print "-g : tells the program to request the password. (This option does alter the program's behavior in the current version. Please use a GPG Agent for the moment.)\n";
	die;
}
