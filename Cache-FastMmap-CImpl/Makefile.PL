use 5.008;
use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'          => 'Cache::FastMmap::CImpl',
    'AUTHOR'        => 'Rob Mueller <cpan@robm.fastmail.fm>',
    'VERSION_FROM'  => 'CImpl.pm',
    'LIBS'          => [''],
    'DEFINE'        => '-DDEBUG',
    'INC'           => '-I.',
    'OBJECT'        => 'CImpl.o mmap_cache.o',
#	    'CCFLAGS'       => '-g',
);
